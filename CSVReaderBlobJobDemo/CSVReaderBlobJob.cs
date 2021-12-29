using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Threading.Tasks;
using CsvHelper;
using CSVReaderBlobJobDemo.Helpers;
using Microsoft.Azure.Storage.Blob;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.Extensions.Logging;

namespace CSVReaderBlobJobDemo
{
    public class CSVReaderBlobJob
    {
        [FunctionName(nameof(CSVReaderBlobJob))]
        public async Task Run([BlobTrigger("csv-reader/upload-file/{name}", Connection = "AzureWebJobsStorage")]
        Stream inputBlob, string name, Binder binder, ILogger logger)
        {
            try
            {
                logger.LogInformation($"Blob Trigger started processing message with {nameof(CSVReaderBlobJob)}," +
                    $"name: {name}");

                var outputContainer = await GetCloudBlobContainer(binder);

                var blockBlob = outputContainer.GetBlockBlobReference($"upload-file/{name}");

                List<CSVFileDto> newFile = new List<CSVFileDto>();

                using (var memoryStream = new MemoryStream())
                {
                    blockBlob.DownloadToStreamAsync(memoryStream).GetAwaiter().GetResult();

                    memoryStream.Position = 0;

                    using (var reader = new StreamReader(memoryStream))

                    using (var csv = new CsvReader(reader, CultureInfo.CurrentCulture))
                    {
                        var records = csv.GetRecords<CSVFileDto>();
                        foreach (CSVFileDto record in records)
                        {
                            //newFile.Add(record);
                            Console.WriteLine($"name:{record.Name};department:{record.Department}");
                        }
                    }
                }

                //upload new file into blob
                //using (var stringWriter = new StringWriter())
                //{
                //    using (var csvWriter = new CsvWriter(stringWriter, CultureInfo.InvariantCulture))
                //    {
                //        csvWriter.WriteRecords(newFile);
                //        csvWriter.Flush();

                //        var blob = outputContainer.GetBlockBlobReference($"new-file_{DateTime.Now.ToString("ddMMyyyy")}.csv");
                //        await blob.UploadTextAsync(stringWriter.ToString());
                //    }
                //}

            }
            catch (Exception ex)
            {
                logger.LogError($"Unable to process {name}, Exception:{ex}", nameof(CSVReaderBlobJob));
                throw;
            }
        }
        private async Task<CloudBlobContainer> GetCloudBlobContainer(Binder binder)
        {
            var container = $"csv-reader";

            var attributes = new Attribute[]
            {
                new BlobAttribute($"{container}",FileAccess.ReadWrite),
                new StorageAccountAttribute("AzureWebJobsStorage")
            };

            var outputContainer = await binder.BindAsync<CloudBlobContainer>(attributes);
            await outputContainer.CreateIfNotExistsAsync();
            return outputContainer;
        }
    }
}
