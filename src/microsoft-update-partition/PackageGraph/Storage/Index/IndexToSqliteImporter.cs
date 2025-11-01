using System;
using System.IO;
using System.IO.Compression;
using System.Text;
using Newtonsoft.Json;
using System.Collections.Generic;
using System.IO.Compression;

namespace Microsoft.PackageGraph.Storage.Index
{
    public static class IndexToSqliteImporter
    {
        // 将顶层 JSON 写入 sqlite，key 作为 TEXT，value 作为 BLOB (gzip(JSON(value)))
        public static void ImportIndexStreamToSqlite<T>(Stream jsonStream, string sqliteFilePath, string tableName = "IndexTable")
        {
            // 创建/打开 DB
            var connStr = new Microsoft.Data.Sqlite.SqliteConnectionStringBuilder { DataSource = sqliteFilePath, Mode = Microsoft.Data.Sqlite.SqliteOpenMode.ReadWriteCreate }.ToString();
            using var conn = new Microsoft.Data.Sqlite.SqliteConnection(connStr);
            conn.Open();

            using (var cmd = conn.CreateCommand())
            {
                cmd.CommandText = $"CREATE TABLE IF NOT EXISTS {tableName} (Key TEXT PRIMARY KEY, Value BLOB)";
                cmd.ExecuteNonQuery();
                cmd.CommandText = $"PRAGMA journal_mode = WAL;";
                cmd.ExecuteNonQuery();
            }

            using var sr = new StreamReader(jsonStream, Encoding.UTF8, detectEncodingFromByteOrderMarks: true, bufferSize: 65536);
            using var reader = new JsonTextReader(sr) { SupportMultipleContent = false };
            var serializer = new JsonSerializer();

            if (!reader.Read() || reader.TokenType != JsonToken.StartObject)
                throw new InvalidDataException("Expected start of JSON object");

            while (reader.Read())
            {
                if (reader.TokenType == JsonToken.EndObject) break;

                if (reader.TokenType != JsonToken.PropertyName) continue;
                string key = (string)reader.Value!;

                if (!reader.Read() || reader.TokenType != JsonToken.StartArray)
                    throw new InvalidDataException($"Expected array for key {key}");

                // 写入临时文件以避免把数组全部存内存
                string tmpFile = Path.GetTempFileName();
                try
                {
                    using (var fs = File.Open(tmpFile, FileMode.Create, FileAccess.Write, FileShare.None))
                    using (var tw = new StreamWriter(fs, new UTF8Encoding(false)))
                    using (var jw = new JsonTextWriter(tw))
                    {
                        jw.WriteStartArray();
                        bool first = true;
                        while (reader.Read())
                        {
                            if (reader.TokenType == JsonToken.EndArray) break;
                            var item = serializer.Deserialize<T>(reader);
                            // 逐项序列化到 tmpFile
                            serializer.Serialize(jw, item);
                        }
                        jw.WriteEndArray();
                        jw.Flush();
                    }

                    // 把 tmpFile 压缩并插入 sqlite
                    byte[] compressed;
                    using (var inFs = File.OpenRead(tmpFile))
                    using (var ms = new MemoryStream())
                    using (var gz = new GZipStream(ms, CompressionLevel.Optimal))
                    {
                        inFs.CopyTo(gz);
                        gz.Flush();
                        gz.Close();
                        compressed = ms.ToArray();
                    }

                    using var insertCmd = conn.CreateCommand();
                    insertCmd.CommandText = $"INSERT OR REPLACE INTO {tableName} (Key, Value) VALUES ($k, $v)";
                    insertCmd.Parameters.AddWithValue("$k", key);
                    var p = insertCmd.CreateParameter();
                    p.ParameterName = "$v";
                    p.DbType = System.Data.DbType.Binary;
                    p.Value = compressed;
                    insertCmd.Parameters.Add(p);
                    insertCmd.ExecuteNonQuery();
                }
                finally
                {
                    try { File.Delete(tmpFile); } catch { }
                }
            }

            conn.Close();
        }
    }
}
