using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.DependencyInjection.Extensions;
using Microsoft.Extensions.Logging;
using Microsoft.Extensions.Logging.Abstractions;
using Microsoft.Extensions.Options;

namespace SwarmKeyDb.SwarmConsistency;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddSwarmConsistency(
        this IServiceCollection services,
        IEnumerable<Uri> beeNodeUrls,
        Action<ConsistencyOptions>? configure = null)
    {
        ArgumentNullException.ThrowIfNull(services);
        ArgumentNullException.ThrowIfNull(beeNodeUrls);

        var nodeUrls = beeNodeUrls.ToArray();
        if (nodeUrls.Length == 0)
        {
            throw new ArgumentException("At least one Bee node URL is required.", nameof(beeNodeUrls));
        }

        services.TryAddSingleton<ILogger<ConsistencyVerificationMiddleware>>(NullLogger<ConsistencyVerificationMiddleware>.Instance);
        services.TryAddSingleton<ILogger<BeeConsistencyVerifier>>(NullLogger<BeeConsistencyVerifier>.Instance);
        services.AddOptions<ConsistencyOptions>();
        if (configure is not null)
        {
            services.Configure(configure);
        }

        services.AddSingleton<ISwarmConsistencyVerifier>(sp =>
        {
            var options = sp.GetRequiredService<IOptions<ConsistencyOptions>>().Value;
            var verifiers = nodeUrls
                .Select(url => new BeeConsistencyVerifier(new HttpClient { BaseAddress = url }, options))
                .Cast<ISwarmConsistencyVerifier>()
                .ToArray();
            if (verifiers.Length == 1)
            {
                return verifiers[0];
            }

            var threshold = options.QuorumThreshold <= 0 ? (verifiers.Length / 2) + 1 : options.QuorumThreshold;
            return new QuorumConsistencyVerifier(verifiers, threshold);
        });

        DecorateKeyValueStore(services);
        return services;
    }

    public static IServiceCollection UseSwarmConsistency(
        this IServiceCollection services,
        IEnumerable<Uri> beeNodeUrls,
        Action<ConsistencyOptions>? configure = null) =>
        AddSwarmConsistency(services, beeNodeUrls, configure);

    private static void DecorateKeyValueStore(IServiceCollection services)
    {
        var descriptor = services.LastOrDefault(static service => service.ServiceType == typeof(IKeyValueStore));
        if (descriptor is null)
        {
            throw new InvalidOperationException("IKeyValueStore must be registered before calling AddSwarmConsistency.");
        }

        services.Remove(descriptor);
        services.AddSingleton<IKeyValueStore>(sp =>
        {
            var inner = CreateFromDescriptor(sp, descriptor);
            var verifier = sp.GetRequiredService<ISwarmConsistencyVerifier>();
            var options = sp.GetRequiredService<IOptions<ConsistencyOptions>>();
            var logger = sp.GetService<ILogger<ConsistencyVerificationMiddleware>>() ?? NullLogger<ConsistencyVerificationMiddleware>.Instance;
            return new ConsistencyVerificationMiddleware(inner, verifier, options, logger);
        });
    }

    private static IKeyValueStore CreateFromDescriptor(IServiceProvider sp, ServiceDescriptor descriptor)
    {
        if (descriptor.ImplementationInstance is IKeyValueStore instance)
        {
            return instance;
        }

        if (descriptor.ImplementationFactory is not null)
        {
            return (IKeyValueStore)descriptor.ImplementationFactory(sp);
        }

        if (descriptor.ImplementationType is not null)
        {
            return (IKeyValueStore)ActivatorUtilities.CreateInstance(sp, descriptor.ImplementationType);
        }

        throw new InvalidOperationException("Unsupported IKeyValueStore registration descriptor.");
    }
}
