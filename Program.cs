using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using Microsoft.WindowsAzure.Storage.Blob;
using Mono.Options;

namespace SqlAzureBak
{
    internal static class Program
    {
        private static readonly CancellationTokenSource Ctc = new CancellationTokenSource();

        private static int Main(string[] args)
        {
            Console.CancelKeyPress += (_, e) =>
                                          {
                                              e.Cancel = true;
                                              Ctc.Cancel();
                                          };

            var cmd = new Dictionary<string, object> {{"file", null}, {"blob", null}};

            var p = new OptionSet
                        {
                            {
                                "cs=|connectionString=", "source connection string",
                                s => cmd["cs"] = s
                            },
                            {
                                "db=|databaseName=", "database name",
                                s => cmd["db"] = s
                            },
                            {
                                "cont=|container=", "blob container name (optional, defaults to databaseName)",
                                s => cmd["blobContainer"] = s
                            },
                            {
                                "f=|file=", "destination file (can be empty, a temp file will be used)",
                                s => cmd["file"] = s
                            },
                            {
                                "b=|blob=",
                                "optionally upload destination file to Azure Blob Storage ('account,key' or 'dev')",
                                s => cmd["blobAccount"] = string.IsNullOrWhiteSpace(s) ? null : ParseAccount(s)
                            },
                            {
                                "h|help", "show this message and exit",
                                s => cmd["help"] = s
                            }
                        };

            p.Parse(args);

            if (cmd.ContainsKey("help") && cmd["help"] != null)
            {
                ShowHelp(p);
                return 0;
            }

            if (cmd.ContainsKey("cs") && cmd.ContainsKey("db"))
            {
                var database = (string) cmd["db"];

                CloudBlobContainer container = null;

                if (cmd.ContainsKey("blobAccount"))
                {
                    var account = (CloudStorageAccount) cmd["blobAccount"];
                    string name = cmd.ContainsKey("blobContainer") ? (string) cmd["blobContainer"] : database;
                    container = account.CreateCloudBlobClient().GetContainerReference(name.ToLowerInvariant());
                }

                return MainExec((string) cmd["cs"], database, (string) cmd["file"], container).Result;
            }

            ShowHelp(p);
            return 0;
        }


        private static async Task<int> MainExec(
            string connectionString, string databaseName, string filePath, CloudBlobContainer container)
        {
            Exception ex = null;

            var exporter = new Exporter(connectionString, databaseName, filePath, container, Ctc.Token);

            try
            {
                await exporter.Prepare();
                await exporter.Export();
            }
            catch (Exception e)
            {
                ex = e;
            }

            if (Ctc.IsCancellationRequested)
            {
                Console.WriteLine("User cancelled");
                return 1;
            }

            if (ex != null)
            {
                Console.Error.WriteLine("General error");
                Console.Error.WriteLine(ex);
                return 100;
            }

            return 0;
        }

        private static CloudStorageAccount ParseAccount(string str)
        {
            if ("dev".Equals(str, StringComparison.OrdinalIgnoreCase))
                return CloudStorageAccount.DevelopmentStorageAccount;

            string[] arr = str.Split(',');
            return new CloudStorageAccount(new StorageCredentials(arr[0], arr[1]), true);
        }

        private static void ShowHelp(OptionSet p)
        {
            Console.WriteLine("SqlAzureBak");
            Console.WriteLine("Export SQL Azure databases as bacpac backup.");
            Console.WriteLine();

            p.WriteOptionDescriptions(Console.Out);
        }
    }
}