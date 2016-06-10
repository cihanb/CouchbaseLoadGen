using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Couchbase;
using Couchbase.Core;
using Couchbase.Configuration.Client;
using System.Diagnostics;
using Couchbase.N1QL;
using System.Configuration;

namespace ConsoleApplication1
{
    class Program
    {
        //startup options 
        //n1ql A 1 100 "select * from default where a1=$1" 0
        //populate K 1 100 1 10 0
        static void Main(string[] args)
        {
            var config = new ClientConfiguration
            {
                Servers = new List<Uri> { 
                    new Uri("http://10.0.0.45:8091/pools")
                },
                UseSsl = false,
                DefaultOperationLifespan = 1000,
                BucketConfigs = new Dictionary<string, BucketConfiguration>
                {
                  {"default", new BucketConfiguration
                  {
                      BucketName = "default",
                      UseSsl = false,
                      Password = "",
                      DefaultOperationLifespan = 2000,
                      PoolConfiguration = new PoolConfiguration
                      {
                        MaxSize = 10,
                        MinSize = 5,
                        SendTimeout = 12000
                      }
                  }
                 }
                }
            };

            //timing definitions
            Stopwatch sw = new Stopwatch();
            long avg_ns_operation = 0;
            long last_ns_operation = 0;

            //parse commandline

            if (args.Length == 0)
            {
                Console.WriteLine("0:operation_mode - can be 'populate' or 'n1ql'");
                return;
            };

            //parse commandline
            string operation_mode = args[0].ToString();
            if (operation_mode == "populate")
            {
                if (args.Length == 1)
                {
                    Console.WriteLine("MODE:populate 1:key_prefix 2:key_range_start 3:key_range_end 4:modulo_adjustor 5:value_size 6:loop");
                    return;
                };

                string key_prefix = args[1].ToString();
                long key_range_start = long.Parse(args[2].ToString());
                long key_range_end = long.Parse(args[3].ToString());
                long selectivity_divider = long.Parse(args[4].ToString());
                int value_size_bytes = int.Parse(args[5].ToString());
                long loop = long.Parse(args[6].ToString());

                //set the mod value based on selectivity
                long mod_selectivity = (long)((key_range_end - key_range_start) / selectivity_divider);


                Cluster cbCluster = new Cluster(config);
                Document<object> cbDoc = new Document<dynamic> { };
                string _key;
                string _id;
                long _a1;
                string _a2 = new string('C', value_size_bytes);
                string[] _a3;
                List<dynamic> _subDocArray;


                using (var cbBucket = cbCluster.OpenBucket())
                {
                    for (long i = key_range_start; i <= key_range_end || loop != 0; i++)
                    {
                        try {
                            _key = key_prefix + "_" + i.ToString();
                            _a1 = DateTime.Now.Ticks % mod_selectivity;
                            _id = Guid.NewGuid().ToString();
                            _a3 = new string[3] { (_a1 % 3).ToString(), (_a1 % 5).ToString(), (_a1 % 7).ToString() };
                            _subDocArray = new List<dynamic> {
                            new { x1 = Guid.NewGuid().ToString(), x2 = (_a1 % 7).ToString() },
                            new { x1 = Guid.NewGuid().ToString(), x2 = (_a1 % 5).ToString() } };
                            cbDoc = new Document<dynamic>
                            {
                                Id = _key,
                                Content = new
                                {
                                    id = _id,
                                    a1 = _a1,
                                    a2 = _a2,
                                    a3 = _a3,
                                    a4 = _subDocArray,
                                    //exp_datetime = DateTime.UtcNow.AddMilliseconds(30000)
                                }
                            };

                            //UPSERT
                            //Console.WriteLine(cbDoc.Content.ToString());
                            //cbDoc.Expiry = 30000;
                            sw.Start();
                            //var upsert = cbBucket.Upsert(cbDoc);
                            var upsert = cbBucket.Insert(cbDoc);
                            sw.Stop();

                            //timings
                            avg_ns_operation = (((avg_ns_operation * (9)) + sw.ElapsedTicks / (TimeSpan.TicksPerMillisecond / 1000)) / (10));
                            last_ns_operation = sw.ElapsedTicks / (TimeSpan.TicksPerMillisecond / 1000);
                            Console.WriteLine("Last Mutation Latency in nano-seconds: " + sw.ElapsedTicks / (TimeSpan.TicksPerMillisecond / 1000) + " :: Avg Latency in nano-secondas " + avg_ns_operation);
                            sw.Reset();

                            if (!upsert.Success) {
                                throw new Exception("upsert failed: " + upsert.Exception.ToString());
                            }

                            //waitfor a second
                            //System.Threading.Thread.Sleep(500);
                        }
                        catch (Exception ex)
                        {
                            Console.WriteLine(ex.ToString());
                        }
                    }
                    //Console.ReadKey();
                }

            }
            else if (operation_mode == "n1ql")
            {
                if (args.Length == 1)
                {
                    Console.WriteLine("MODE:n1ql 1:value_prefix 2:value_range_start 3:valuey_range_end 4:query_string 5:loop");
                    return;
                };

                string value_prefix = args[1].ToString();
                if (value_prefix == "null") value_prefix = "";
                long value_range_start = long.Parse(args[2].ToString());
                long value_range_end = long.Parse(args[3].ToString());
                string query_string = args[4].ToString();
                long loop = long.Parse(args[5].ToString());

                Cluster cbCluster = new Cluster(config);
                using (var cbBucket = cbCluster.OpenBucket())
                {
                    //string query;
                    //query = query_string.Replace("$1", value_prefix + i.ToString());
                    IQueryRequest qrequest;
                    //get query ready
                    qrequest = QueryRequest.Create(query_string); //query_string is something like select * from bucket where attrib=$1
                    qrequest.ScanConsistency(ScanConsistency.RequestPlus);
                    qrequest.AdHoc(true);
                    //qrequest.AddPositionalParameter(value_prefix + i.ToString());

                    for (long i = value_range_start; i < value_range_end || loop != 0; i++)
                    {
                        //execute
                        sw.Start();
                        //try
                        //{
                            var result = cbBucket.Query<dynamic>(qrequest);
                            //just fetch and throw away
                            foreach (var item in result.Rows)
                            { }
                        //}
                        //catch when (true)
                        //{ };

                        sw.Stop();

                        //timings
                        if (i != value_range_start)
                            avg_ns_operation = (((avg_ns_operation * (99)) + sw.ElapsedMilliseconds) / (100));
                        else
                            avg_ns_operation = sw.ElapsedMilliseconds;

                        last_ns_operation = sw.ElapsedMilliseconds;
                        Console.WriteLine("Last Query Latency in milli-seconds: " + sw.ElapsedMilliseconds + " :: Avg Query Latency in milli-seconds " + avg_ns_operation);
                        sw.Reset();

                        //if (!result.Success) { throw new Exception("query failed"); }
                    }
                }
            }

            if (operation_mode == "view")
            {
                if (args.Length == 1)
                {
                    Console.WriteLine("MODE:view 1:value_prefix 2:value_range_start 3:valuey_range_end 4:ddoc:view 5:loop");
                    return;
                };

                string value_prefix = args[1].ToString();
                if (value_prefix == "null") value_prefix = "";
                long value_range_start = long.Parse(args[2].ToString());
                long value_range_end = long.Parse(args[3].ToString());
                string ddoc_view_name = args[4].ToString();
                long loop = long.Parse(args[5].ToString());



                Cluster cbCluster = new Cluster(config);
                using (var cbBucket = cbCluster.OpenBucket())
                {
                    
                    for (long i = value_range_start; i < value_range_end || loop != 0; i++)
                    {
                        var viewquery = cbBucket.CreateQuery(ddoc_view_name.Split(':')[0], ddoc_view_name.Split(':')[1]);
                        viewquery.Stale(Couchbase.Views.StaleState.Ok);
                        viewquery.Key(value_prefix + i.ToString(), false);



                        sw.Start();
                        var result = cbBucket.Query<dynamic>(viewquery);
                        foreach (var item in result.Rows)
                        { }
                        sw.Stop();

                        //timings
                        avg_ns_operation = (((avg_ns_operation * (9)) + sw.ElapsedMilliseconds) / (10));
                        last_ns_operation = sw.ElapsedMilliseconds;
                        Console.WriteLine("Last Mutation Latency in milli-seconds: " + sw.ElapsedMilliseconds + " :: Avg Latency in milli-secondas " + avg_ns_operation);
                        sw.Reset();

                        if (!result.Success) { throw new Exception("query failed"); }
                    }
                }
            }

        }
    }
}
