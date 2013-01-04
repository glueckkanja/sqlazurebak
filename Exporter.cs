using System;
using System.IO;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.SqlServer.Dac;
using Microsoft.WindowsAzure.Storage.Blob;

namespace SqlAzureBak
{
    internal class Exporter
    {
        private readonly string _connectionString;
        private readonly CloudBlobContainer _container;
        private readonly string _databaseName;
        private readonly CancellationToken _token;

        private bool _deleteFile;
        private string _filePath;

        public Exporter(string connectionString, string databaseName, string filePath, CloudBlobContainer container,
                        CancellationToken token)
        {
            _connectionString = connectionString;
            _databaseName = databaseName;
            _filePath = filePath;
            _container = container;
            _token = token;
        }

        public async Task Prepare()
        {
            if (_container != null)
            {
                await _container.CreateIfNotExistsAsync(_token);
            }
        }

        public async Task Export()
        {
            var services = new DacServices(_connectionString);

            services.ProgressChanged += ProgressChangedOnMessage;

            string blobName;

            if (string.IsNullOrEmpty(_filePath))
            {
                _filePath = Path.GetTempFileName();
                _deleteFile = true;
                blobName = string.Format("{0:yyyyMMddTHHmmss}-{1}.bacpac", DateTimeOffset.UtcNow, _databaseName);
            }
            else
            {
                blobName = Path.GetFileName(_filePath);
            }

            try
            {
                using (FileStream stream = File.Open(_filePath, FileMode.Create, FileAccess.ReadWrite))
                {
                    Console.WriteLine("starting bacpac export");

                    await services.ExportBacpacAsync(stream, _databaseName, _token);
                    await stream.FlushAsync();

                    stream.Seek(0, SeekOrigin.Begin);

                    if (_container != null && !_token.IsCancellationRequested)
                    {
                        Console.WriteLine("uploading to azure blob storage");
                        CloudBlockBlob blob = _container.GetBlockBlobReference(blobName);
                        await blob.UploadFromStreamAsync(stream, _token);
                    }
                }
            }
            finally
            {
                Cleanup();
            }
        }

        private void Cleanup()
        {
            if (_deleteFile)
            {
                Console.WriteLine("cleaning up");
                File.Delete(_filePath);
            }
        }

        private void ProgressChangedOnMessage(object sender, DacProgressEventArgs e)
        {
            Console.WriteLine(e.Message);
        }
    }
}