using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.IO.Compression;
using System.Text;
using Microsoft.Data.Sqlite;
using Newtonsoft.Json;

namespace Microsoft.PackageGraph.Storage.Index
{
    public interface IDiskIndex<I, T> : IDisposable
    {
        void Add(I key, T value);
        bool TryGet(I key, out T value);
        IEnumerable<KeyValuePair<I, T>> GetAllEntries();
    }

    // DiskBackedIndex: 使用 SQLite 存储 Key -> gzip(json(value)) blob
    public class DiskBackedIndex<I, T> : IDiskIndex<I, T>
    {
        private readonly SqliteConnection _conn;
        private readonly string _tableName;
        private readonly JsonSerializer _serializer = new JsonSerializer();
        private readonly Func<I, string> _keySerializer;
        private readonly Func<string, I> _keyDeserializer;

        public DiskBackedIndex(string sqliteFilePath, string tableName = "IndexTable",
            Func<I, string> keySerializer = null, Func<string, I> keyDeserializer = null)
        {
            _tableName = string.IsNullOrWhiteSpace(tableName) ? "IndexTable" : tableName;
            _keySerializer = keySerializer ?? (k => k.ToString()!);
            _keyDeserializer = keyDeserializer ?? (s => (I)Convert.ChangeType(s, typeof(I)));

            var csb = new SqliteConnectionStringBuilder { DataSource = sqliteFilePath, Mode = SqliteOpenMode.ReadWriteCreate }.ToString();
            _conn = new SqliteConnection(csb);
            _conn.Open();

            using var cmd = _conn.CreateCommand();
            cmd.CommandText = $"CREATE TABLE IF NOT EXISTS {_tableName} (Key TEXT PRIMARY KEY, Value BLOB)";
            cmd.ExecuteNonQuery();

            using var cmd2 = _conn.CreateCommand();
            cmd2.CommandText = $"PRAGMA journal_mode = WAL;";
            cmd2.ExecuteNonQuery();
        }

        private static byte[] SerializeAndCompress(T val, JsonSerializer serializer)
        {
            using var ms = new MemoryStream();
            using (var gz = new GZipStream(ms, CompressionLevel.Optimal, leaveOpen: true))
            using (var sw = new StreamWriter(gz, Encoding.UTF8, 65536, leaveOpen: true))
            using (var jw = new JsonTextWriter(sw))
            {
                serializer.Serialize(jw, val);
                jw.Flush();
                sw.Flush();
                gz.Flush();
            }
            return ms.ToArray();
        }

        private static T DecompressAndDeserialize(byte[] data, JsonSerializer serializer)
        {
            using var ms = new MemoryStream(data);
            using var gz = new GZipStream(ms, CompressionMode.Decompress);
            using var sr = new StreamReader(gz, Encoding.UTF8);
            using var jr = new JsonTextReader(sr);
            return serializer.Deserialize<T>(jr);
        }

        public void Add(I key, T value)
        {
            var keyStr = _keySerializer(key);
            var blob = SerializeAndCompress(value, _serializer);

            using var tran = _conn.BeginTransaction();
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = $"INSERT OR REPLACE INTO {_tableName} (Key, Value) VALUES ($k, $v)";
            cmd.Parameters.AddWithValue("$k", keyStr);
            var p = cmd.CreateParameter();
            p.ParameterName = "$v";
            p.DbType = DbType.Binary;
            p.Value = blob;
            cmd.Parameters.Add(p);
            cmd.ExecuteNonQuery();
            tran.Commit();
        }

        public bool TryGet(I key, out T value)
        {
            var keyStr = _keySerializer(key);
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = $"SELECT Value FROM {_tableName} WHERE Key = $k";
            cmd.Parameters.AddWithValue("$k", keyStr);
            using var reader = cmd.ExecuteReader();
            if (!reader.Read())
            {
                value = default!;
                return false;
            }
            var blob = (byte[])reader["Value"];
            value = DecompressAndDeserialize(blob, _serializer);
            return true;
        }

        public IEnumerable<KeyValuePair<I, T>> GetAllEntries()
        {
            using var cmd = _conn.CreateCommand();
            cmd.CommandText = $"SELECT Key, Value FROM {_tableName}";
            using var reader = cmd.ExecuteReader();
            while (reader.Read())
            {
                var keyStr = reader.GetString(0);
                var blob = (byte[])reader[1];
                var key = _keyDeserializer(keyStr);
                var val = DecompressAndDeserialize(blob, _serializer);
                yield return new KeyValuePair<I, T>(key, val);
            }
        }

        public void Dispose()
        {
            _conn?.Dispose();
        }
    }
}
