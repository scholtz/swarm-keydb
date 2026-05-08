namespace SwarmKeyDb;

/// <summary>Store operation kinds checked by DID authorization.</summary>
public enum DidOperation
{
    /// <summary>Read a value from the store.</summary>
    Read,

    /// <summary>Write (put) a value into the store.</summary>
    Write,

    /// <summary>Delete a value from the store.</summary>
    Delete
}
