using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dac;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;

namespace SqlAzureBak
{
    public static class StorageExtensions
    {
        public static Task<bool> CreateIfNotExistsAsync(
            this CloudBlobContainer container, CancellationToken ct = default(CancellationToken))
        {
            ICancellableAsyncResult ar = container.BeginCreateIfNotExists(null, null);
            ct.Register(ar.Cancel);

            return Task.Factory.FromAsync<bool>(ar, container.EndCreateIfNotExists);
        }

        public static Task ExportBacpacAsync(
            this DacServices services, Stream stream, string database, CancellationToken ct = default(CancellationToken))
        {
            return Task.Factory.StartNew(() => services.ExportBacpac(stream, database, cancellationToken: ct));
        }

        public static Task UploadFromStreamAsync(
            this CloudBlockBlob blob, Stream stream, CancellationToken ct = default(CancellationToken))
        {
            ICancellableAsyncResult ar = blob.BeginUploadFromStream(stream, null, null);
            ct.Register(ar.Cancel);
            return Task.Factory.FromAsync(ar, blob.EndUploadFromStream);
        }
    }
}