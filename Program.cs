using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.ServiceProcess;
using System.Threading.Tasks;
using System.Data.SqlClient;
using System.Data;
using System.Diagnostics;
using System.Threading;
using System.Text.RegularExpressions;

namespace Bonus1_CSharp
{
    class Program
    {
        static void Main(string[] args)
        {
            int count = 1000000; // should 
            var dbRunner = new DatabaseRunner(count, 10);
            //dbRunner.RestartSQLServer(); // To be tested
            //Console.WriteLine($"Insert {count} records");
            //dbRunner.SingleThreadInsertTest();
            //for (int i = 1; i <= dbRunner.ProcessorCount; i++)
            //    dbRunner.MultiThreadInsertTest(i);
            //for (int i = 1; i <= dbRunner.ProcessorCount; i++)
            //    dbRunner.MultiThreadInsertTest_batch(i);
            //dbRunner.MultiThreadInsertTest_batch(8);

            //string commandText = "SELECT * FROM test WHERE uuid=10 or uuid=100 or uuid=1000 or uuid=10000 or uuid=100000 or uuid=1000000 or uuid = 10000000;";
            string commandText = "SELECT * FROM test WHERE uuid=10;";
            for (int i = 0; i <= 6; i++)
                dbRunner.MultiThreadSelectTest(1 << i, commandText);
            //for (int i = 0; i <= 6; i++)
            //    dbRunner.MultiThreadSelectTest(1 << i);
        }
    }

    class DatabaseRunner
    {
        public int ProcessorCount { get; private set; }
        int operationNum;
        int selectRetryNum;
        private string username, password;
        SqlConnection sqlConnection;

        public DatabaseRunner(int oNum, int sltReNum = 10, string usrname = "", string passwd = "") // 重载构造函数，接受总操作数（行数）与线程数
        {
            this.ProcessorCount = Environment.ProcessorCount; // 读取系统信息
            this.username = usrname;
            this.password = passwd;
            this.operationNum = oNum;
            this.selectRetryNum = sltReNum;
            sqlConnection = new SqlConnection // 建立默认SQL连接
            {
                ConnectionString = "Data Source=.;Initial Catalog=bonus1;Integrated Security=True;Pooling=False"
            };
            sqlConnection.Open();
        }

        public void RestartSQLServer()
        {
            ServiceController sc = new ServiceController("MSSQLSERVER");
            if (!sc.Status.Equals(ServiceControllerStatus.Stopped))
            {
                sc.Stop();
            }
            if (sc.Status.Equals(ServiceControllerStatus.Stopped))
            {
                sc.Start();
            }
        }

        public void InitializeDatabase() // 初始化数据库（删除所有数据并重建）
        {
            var command = sqlConnection.CreateCommand();
            command.CommandText = $@"DROP TABLE IF EXISTS test;
                                     CREATE TABLE test (
                                     id BIGINT NOT NULL,
                                     uuid VARCHAR(255) NOT NULL
                                     PRIMARY KEY (id)
                                     )";
            command.ExecuteNonQuery();
        } // reconstruct the database

        public TimeSpan SingleThreadInsertTest() // 单线程插入，速度非常慢。
        {
            var stopwatch = new Stopwatch();
            var command = sqlConnection.CreateCommand();
            InitializeDatabase();
            stopwatch.Start();
            for (int i = 0; i < operationNum; i++)
            {
                command.CommandText = $"INSERT INTO test (id, uuid) VALUES ({i}, {operationNum - i})";
                command.ExecuteNonQuery();
            }
            stopwatch.Stop();
            Console.WriteLine($"Single thread Insert ({operationNum}):\t\t" + stopwatch.Elapsed.ToString());
            return stopwatch.Elapsed;
        }

        public TimeSpan MultiThreadInsertTest(int threadNum, int mode = 0) // 多线程写入，mode为插入线程池的方式为正常循环/并发循环，具体分析见文档
        {
            //var ct = new CancellationTokenSource();
            var stopwatch = new Stopwatch();
            int toProcess = operationNum;
            InitializeDatabase();
            ThreadPool.SetMinThreads(threadNum, threadNum); // 设置线程数
            ThreadPool.SetMaxThreads(threadNum, threadNum);

            stopwatch.Start();
            using (ManualResetEvent resetEvent = new ManualResetEvent(false))
            {
                WaitCallback insert = (i) => // 新建匿名函数
                {
                    using (var tmpConnection = new SqlConnection
                    {
                        ConnectionString = $"Data Source=.;Initial Catalog=bonus1;Integrated Security=True;Max Pool Size={threadNum};Min Pool Size={threadNum}" // Max Pool Size=512;Connect Timeout=500;
                        // ConnectionString = $"Data Source=.;Initial Catalog=bonus1;Integrated Security=True;Pooling=false;" // Max Pool Size=512;Connect Timeout=500;
                    })
                    {
                        tmpConnection.Open();
                        var tmpCommand = tmpConnection.CreateCommand();
                        tmpCommand.CommandText = $"INSERT INTO test (id, uuid) VALUES ({(int)i}, {operationNum - (int)i})"; // 新建查询
                        tmpCommand.ExecuteNonQuery();
                        if (Interlocked.Decrement(ref toProcess) == 0) // 保证所有线程运行完毕
                            resetEvent.Set();
                    }
                };
                if (mode == 1) // 将任务推入线程池
                    Parallel.For(0, operationNum, (i) => ThreadPool.QueueUserWorkItem(insert, i));
                else
                    for (int i = 0; i < operationNum; i++)
                        ThreadPool.QueueUserWorkItem(insert, i);
                resetEvent.WaitOne(); // 等待所有线程结束
            }
            //ct.Cancel();
            stopwatch.Stop();
            Console.WriteLine($"Multi thread Insert({threadNum} with parser):\t" + stopwatch.Elapsed.ToString());
            return stopwatch.Elapsed;
        }

        public TimeSpan MultiThreadInsertTest_batch(int threadNum, int mode = 0) // 多线程批量插入数据库
        {
            //var ct = new CancellationTokenSource();
            var stopwatch = new Stopwatch();
            int toProcess = threadNum;
            InitializeDatabase(); // 清空数据库
            ThreadPool.SetMinThreads(threadNum, threadNum); // 指定线程数
            ThreadPool.SetMaxThreads(threadNum, threadNum);

            stopwatch.Start();
            using (ManualResetEvent resetEvent = new ManualResetEvent(false))
            {
                WaitCallback insert = (oindex) =>
                {
                    Stopwatch stopwatch_tmp = new Stopwatch();
                    stopwatch_tmp.Start();
                    int index = (int)oindex;

                    int lowerbound = operationNum / threadNum * index;
                    int upperbound = index == threadNum - 1 ? operationNum - 1 : operationNum / threadNum * (index + 1); // 拆分成threadNum个子插入
                    DataTable dataTable = new DataTable(); // 新建DataTable，辅助插入
                    dataTable.Columns.AddRange(new DataColumn[] { new DataColumn("id", typeof(long)), new DataColumn("uuid", typeof(string)) });
                    for (int i = lowerbound; i < upperbound; i++) // 建立DataTable
                    {
                        var dr = dataTable.NewRow();
                        dr[0] = i;
                        dr[1] = operationNum - i;
                        dataTable.Rows.Add(dr);
                    }
                    if (dataTable != null && dataTable.Rows.Count != 0) // 将DataTable插入数据库
                    {
                        var tmpConnection = new SqlConnection
                        {
                            ConnectionString = $"Data Source=.;Initial Catalog=bonus1;Integrated Security=True;Connect Timeout=300;Min Pool Size={threadNum}; Max Pool Size={threadNum};"
                            // ConnectionString = $"Data Source=.;Initial Catalog=bonus1;Integrated Security=True;Pooling=false;" // Max Pool Size=512;Connect Timeout=500;                                                                                                
                        };
                        tmpConnection.Open();
                        SqlBulkCopy bulkCopy = new SqlBulkCopy(tmpConnection, SqlBulkCopyOptions.Default, null) // 使用SqlBulkCopy完成批量插入
                        {
                            DestinationTableName = "test",
                            BatchSize = upperbound - lowerbound,
                            BulkCopyTimeout = 300 // 设置Timeout，防止超时
                        };
                        bulkCopy.WriteToServer(dataTable);
                        tmpConnection.Close();
                        GC.Collect();
                    }
                    Console.WriteLine(stopwatch_tmp.Elapsed);
                    if (Interlocked.Decrement(ref toProcess) == 0) // 确认所有进程执行完毕
                        resetEvent.Set();
                };
                for (int i = 0; i < threadNum; i++)
                    ThreadPool.QueueUserWorkItem(insert, i);
                resetEvent.WaitOne(); // 等待所有进程执行完毕
            }
            //ct.Cancel();
            stopwatch.Stop();
            Console.WriteLine($"Multi thread Insert (bulk, {threadNum}):\t" + stopwatch.Elapsed.ToString());
            return stopwatch.Elapsed;
        }

        public TimeSpan SingleThreadSelectTest() // 单线程进行选择操作
        {
            var stopwatch = new Stopwatch();
            Random r = new Random();
            var command = sqlConnection.CreateCommand();
            stopwatch.Start(); // 计时开始
            for (int i = 0; i < selectRetryNum; i++)
            {
                //var number = r.Next(1, operationNum);
                //command.CommandText = $"SELECT * FROM test WHERE uuid={number}";
                command.CommandText = $"SELECT * FROM test"; // 执行全部读取指令
                var reader = command.ExecuteReader();
                reader.Close();
            }
            stopwatch.Stop(); // 计时结束
            Console.WriteLine($"Single thread Select ({operationNum}):\t\t" + stopwatch.Elapsed.ToString());
            return stopwatch.Elapsed;
        }

        public TimeSpan MultiThreadSelectTest(int threadNum) // 多线程进行选择，本重载不含有下推
        {
            var command = sqlConnection.CreateCommand();
            ThreadPool.SetMinThreads(threadNum, threadNum); // 指定线程数
            ThreadPool.SetMaxThreads(threadNum, threadNum);
            // Random r = new Random();
            var stopwatch = new Stopwatch(); // 开始计时
            stopwatch.Start();
            for (int i = 0; i < selectRetryNum; i++) // 循环指定次数
            {
                bool found = false; // 重置found变量，该变量用于检查是否正确搜索
                var toProcess = threadNum;
                //var number = r.Next(1, operationNum);
                using (ManualResetEvent resetEvent = new ManualResetEvent(false))
                {
                    WaitCallback insert = (index) =>
                    {
                        using (var tmpConnection = new SqlConnection
                        {
                            ConnectionString = $"Data Source=.;Initial Catalog=bonus1;Integrated Security=True;Max Pool Size={threadNum};Min Pool Size={threadNum};" // Max Pool Size=512;Connect Timeout=500;
                        })
                        {
                            tmpConnection.Open();
                            var tmpCommand = tmpConnection.CreateCommand();
                            tmpCommand.CommandTimeout = 300;
                            int lowerbound = operationNum / threadNum * (int)index;
                            int upperbound = (int)index == threadNum - 1 ? operationNum - 1 : operationNum / threadNum * ((int)index + 1); // 将搜索域划分为threadNum个区间
                            tmpCommand.CommandText = $@"SELECT * FROM test WHERE id>={lowerbound} and id<{upperbound}";
                            var reader = tmpCommand.ExecuteReader();
                            if (reader.Read())
                            {
                                found = true;
                            }
                            reader.Close();
                            if (Interlocked.Decrement(ref toProcess) == 0) // 保证所有线程运行结束
                            {
                                resetEvent.Set(); // 重置resetEvent
                                if (found == false) // 用于debug，如果未找到就退出，说明搜索条件或代码存在问题
                                    Console.WriteLine($"Error @ multi-thread select");
                            }
                            //Console.WriteLine(toProcess);
                        }
                    };
                    //Parallel.For(0, threadNum, (index) =>
                    //    ThreadPool.QueueUserWorkItem(insert, index)
                    //);
                    for (int k = 0; k < threadNum; k++) // 循环将threadNum个任务推入线程池
                        ThreadPool.QueueUserWorkItem(insert, k);

                    resetEvent.WaitOne(); // 等待运行结束
                }
                // pause Console.Read();
            }
            stopwatch.Stop();
            Console.WriteLine($"Multi thread {threadNum,2}:\t" + stopwatch.Elapsed.ToString());
            return stopwatch.Elapsed;
        }

        public TimeSpan MultiThreadSelectTest(int threadNum, string commandText) //带parser和下推的
        {
            var stopwatch = new Stopwatch();
            ThreadPool.SetMinThreads(threadNum, threadNum); // 指定线程数
            ThreadPool.SetMaxThreads(threadNum, threadNum);
            string pattern = @"WHERE(.*?);"; // 用正则表达式匹配下推的条件
            var match = Regex.Match(commandText, pattern);
            string statement = match.Groups[1].Value; // 赋值寻找条件
            stopwatch.Start(); // 开始计时
            for (int i = 0; i < selectRetryNum; i++) // 循环指定次数
            {
                bool found = false;
                var toProcess = threadNum; // 以上两个变量用于判断是否进程全部结束
                using (ManualResetEvent resetEvent = new ManualResetEvent(false))
                {
                    void insert(object index)
                    {
                        using (var tmpConnection = new SqlConnection
                        {
                            ConnectionString = $"Data Source=.;Initial Catalog=bonus1;Integrated Security=True;Max Pool Size={threadNum};Min Pool Size={threadNum};" // Max Pool Size=512;Connect Timeout=500;
                        })
                        {
                            //Stopwatch tmp_stopwatch = new Stopwatch();
                            //tmp_stopwatch.Start();
                            tmpConnection.Open();
                            var tmpCommand = tmpConnection.CreateCommand();
                            tmpCommand.CommandTimeout = 300;
                            int lowerbound = operationNum / threadNum * (int)index; // 将查询拆分为threadNum个子查询
                            int upperbound = (int)index == threadNum - 1 ? operationNum - 1 : operationNum / threadNum * ((int)index + 1);
                            // tmpCommand.CommandText = $@"SELECT * FROM test WHERE id>={lowerbound} and id<{upperbound} and {statement}";
                            tmpCommand.CommandText = $@"SELECT * FROM test WHERE {statement} and id>={lowerbound} and id<{upperbound};";
                            var reader = tmpCommand.ExecuteReader();
                            if (reader.Read())
                                found = true;
                            reader.Close();
                            //Console.WriteLine(tmp_stopwatch.Elapsed);
                            if (Interlocked.Decrement(ref toProcess) == 0) // 判断是否所有线程运行结束
                            {
                                resetEvent.Set();
                                if (found == false) // 用于debug
                                    Console.WriteLine($"Error @ multi-thread select");
                            }
                        }
                    }
                    //Parallel.For(0, threadNum, (index) =>
                    //    ThreadPool.QueueUserWorkItem(insert, index)
                    //);
                    for (int k = 0; k < threadNum; k++)
                        ThreadPool.QueueUserWorkItem(insert, k);

                    resetEvent.WaitOne(); // 等待所有线程运行结束
                }
            }
            stopwatch.Stop();
            Console.WriteLine($"Multi thread select ({threadNum,2}):\t" + stopwatch.Elapsed.ToString());
            return stopwatch.Elapsed;
        }

        private void CreateSubSelect(int index, int threadNum, int number, out bool found) // 暂时未用到
        {
            found = false;
            using (var tmpConnection = new SqlConnection
            {
                ConnectionString = $"Data Source=.;Initial Catalog=bonus1;Integrated Security=True;Max Pool Size={threadNum};Min Pool Size={threadNum}" // Max Pool Size=512;Connect Timeout=500;
            })
            {
                tmpConnection.Open();
                var tmpCommand = tmpConnection.CreateCommand();
                int lowerbound = operationNum / threadNum * index;
                int upperbound = index == threadNum - 1 ? operationNum - 1 : operationNum / threadNum * (index + 1);
                tmpCommand.CommandText = $@"SELECT * FROM test WHERE id>={lowerbound} and id<{upperbound} and uuid={number}";
                var reader = tmpCommand.ExecuteReader();
                if (reader.Read())
                    found = true;

                reader.Close();
            }
        }
    }
}
