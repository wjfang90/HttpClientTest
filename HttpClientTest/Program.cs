using System;
using System.IO;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Threading.Tasks;

namespace HttpClientTest
{
    class Program
    {

        internal static string ES_Url = "http://192.168.0.151:9215";
        static async Task Main(string[] args)
        {

            await SearchAllEsData();

            await IndexEsData();

            await UpdateEsData();

            await DeleteEsData();

            await BulkEsData();

            Console.ReadKey();
        }


        public async static Task SearchAllEsData()
        {
            try
            {
                var client = new HttpClient();

                client.BaseAddress = new Uri(ES_Url);
                //client.DefaultRequestHeaders.TryAddWithoutValidation("Content-Type", "application/json");

                var contentStr = "{\"query\": {\"bool\": {\"must\": [{\"match_all\": { }}]}},\"from\": 0,\"size\": 10}";

                var request = new HttpRequestMessage(HttpMethod.Post, "test/_search?pretty");

                request.Content = new StringContent(contentStr, Encoding.UTF8, "application/json");//CONTENT-TYPE header
                //request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");


                var response = await client.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    var result = response.Content.ReadAsStringAsync();

                    Console.WriteLine(result.Result);
                }

                client.Dispose();
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async static Task IndexEsData()
        {
            try
            {
                var client = new HttpClient();

                client.BaseAddress = new Uri(ES_Url);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));//accept header

               var contentStr = @"{ ""Gid"":""3333"",""Title"" : ""test index value"" }";

                var request = new HttpRequestMessage(HttpMethod.Put, "test/test/333?pretty");

                request.Content = new StringContent(contentStr, Encoding.UTF8, "application/json");


                var response = await client.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    var result = response.Content.ReadAsStringAsync();

                    Console.WriteLine(result.Result);
                }

                client.Dispose();
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async static Task UpdateEsData()
        {
            try
            {
                var client = new HttpClient();

                client.BaseAddress = new Uri(ES_Url);
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/json"));//accept header

                var contentStr = @"{""doc"":{ ""Gid"":""333"",""Title"" : ""test update value"" }}";

                var request = new HttpRequestMessage(HttpMethod.Post, "test/test/333/_update?pretty");
                request.Content = new StringContent(contentStr, Encoding.UTF8, "application/json");


                var response = await client.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    var result = response.Content.ReadAsStringAsync();

                    Console.WriteLine(result.Result);
                }

                client.Dispose();
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async static Task DeleteEsData()
        {
            try
            {
                var client = new HttpClient();

                client.BaseAddress = new Uri(ES_Url);

                var request = new HttpRequestMessage(HttpMethod.Delete, "test/test/333?pretty");

                var response = await client.SendAsync(request);

                if (response.IsSuccessStatusCode)
                {
                    var result = response.Content.ReadAsStringAsync();

                    Console.WriteLine(result.Result);
                }

                client.Dispose();
            }
            catch (Exception ex)
            {
                throw;
            }
        }

        public async static Task BulkEsData()
        {
            try
            {
                var client = new HttpClient();

                client.BaseAddress = new Uri(ES_Url);
                //设置header accept， ES bulk API 能接收的数据类型是 ndjson
                client.DefaultRequestHeaders.Accept.Add(new MediaTypeWithQualityHeaderValue("application/x-ndjson"));//accept header

                var request = new HttpRequestMessage(HttpMethod.Post, "_bulk?pretty");

                var filePath = Path.Combine(AppDomain.CurrentDomain.BaseDirectory, "ES_bulk.ndjson");
                var contentStr = File.ReadAllText(filePath);

                request.Content = new StringContent(contentStr, Encoding.UTF8, "application/json");//ContentType header
                //request.Content = new StringContent(contentStr, Encoding.UTF8);
                //request.Content.Headers.ContentType = new MediaTypeHeaderValue("application/json");//ContentType header


                var response = await client.SendAsync(request); 

                if (response.IsSuccessStatusCode)
                {
                    var result = response.Content.ReadAsStringAsync();

                    Console.WriteLine(result.Result);
                }

                client.Dispose();
            }
            catch (Exception ex)
            {
                throw;
            }
        }
    }

   
}
