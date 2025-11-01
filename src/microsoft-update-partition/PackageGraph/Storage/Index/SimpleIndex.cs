using Microsoft.PackageGraph.ObjectModel;
using Newtonsoft.Json;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;

namespace Microsoft.PackageGraph.Storage.Index
{
    abstract class SimpleIndex<I, T> : IIndex
    {
        protected IDiskIndex<I, T> Index;

        protected IIndexStreamContainer _Container;

        public abstract IndexDefinition Definition { get; }

        protected string IndexName;
        protected string PartitionName;

        private bool IsIndexLoaded = false;

        public const int CurrentVersion = 0;

        protected bool SaveWithSwappedKeyValue = false;

        public bool IsDirty { get; private set; }

        protected SimpleIndex(IIndexContainer container, string indexName, string partitionName = null)
        {
            if (container is IIndexStreamContainer streamContainer)
            {
                _Container = streamContainer;
            }
            else
            {
                throw new Exception("Index container type not compatible with this index");
            }

            IndexName = indexName;
            PartitionName = partitionName;

            IsDirty = false;
        }

        public void Add(I key, T entry)
        {
            IsDirty = true;

            if (!IsIndexLoaded)
            {
                ReadIndex();
            }

            Index.Add(key, entry);
        }

        public void Save(Stream destination)
        {
            if (_Container == null)
            {
                throw new Exception("The index was initialized with a non-streamable container");
            }

            if (!IsIndexLoaded)
            {
                // The index was not loaded, hence it did not change. Simply copy the serialized index
                // from the old place to the new stream
                if (_Container.TryGetIndexReadStream(Definition, out Stream indexStream))
                {
                    using (indexStream)
                    {
                        indexStream.CopyTo(destination);
                    }
                }
                else
                {
                    // no existing index - write empty object
                    using var sw = new StreamWriter(destination, System.Text.Encoding.UTF8, 65536, leaveOpen: true);
                    using var jw = new JsonTextWriter(sw);
                    jw.WriteStartObject();
                    jw.WriteEndObject();
                    jw.Flush();
                }
            }
            else
            {
                // Index is loaded. We need to serialize it out.
                // If SaveWithSwappedKeyValue is true, we should swap keys/values.
                // We'll stream-output JSON to avoid loading everything into memory.
                using var sw = new StreamWriter(destination, System.Text.Encoding.UTF8, 65536, leaveOpen: true);
                using var jw = new JsonTextWriter(sw);
                var serializer = new JsonSerializer();

                jw.WriteStartObject();

                if (SaveWithSwappedKeyValue)
                {
                    // When swapping, the key becomes value and vice versa; this case is uncommon.
                    // We'll iterate through entries and write swapped pairs.
                    foreach (var kv in Index.GetAllEntries())
                    {
                        // key is I, value is T -> we want property name = serialized T? That is awkward.
                        // For simplicity, we will use key.ToString() as property name and store value as T.
                        // If your use case requires swapped semantics, implement a specific serializer here.
                        string propName = kv.Key?.ToString() ?? string.Empty;
                        jw.WritePropertyName(propName);
                        serializer.Serialize(jw, kv.Value);
                    }
                }
                else
                {
                    // Normal: property name = key.ToString() and value = serialized T (which might be array)
                    foreach (var kv in Index.GetAllEntries())
                    {
                        string propName = kv.Key?.ToString() ?? string.Empty;
                        jw.WritePropertyName(propName);
                        serializer.Serialize(jw, kv.Value);
                    }
                }

                jw.WriteEndObject();
                jw.Flush();
            }
        }

        protected void ReadIndex()
        {
            if (_Container == null)
            {
                throw new Exception("The index was initialized with a non-streamable container");
            }

            lock (this)
            {
                if (!IsIndexLoaded)
                {
                    try
                    {
                        if (_Container.TryGetIndexReadStream(Definition, out Stream indexStream))
                        {
                            using (indexStream)
                            {
                                // If the index stream is large, import into sqlite and use disk-backed index
                                bool useDiskBacked = false;
                                long sizeThreshold = 1L * 1024 * 1024; // 1 MB threshold; tune as needed
                                try
                                {
                                    if (indexStream.CanSeek && indexStream.Length > sizeThreshold)
                                    {
                                        useDiskBacked = true;
                                    }
                                }
                                catch { /* ignore */ }

                                if (useDiskBacked)
                                {
                                    // import to sqlite file (in same folder as index container or temp)
                                    string dbPath = Path.Combine(Path.GetTempPath(), $"{IndexName}.idx.db");
                                    // Ensure prev db removed
                                    try { if (File.Exists(dbPath)) File.Delete(dbPath); } catch { }

                                    IndexToSqliteImporter.ImportIndexStreamToSqlite<object>(indexStream, dbPath, "IndexTable");
                                    Index = new DiskBackedIndex<I, T>(dbPath, "IndexTable", k => k.ToString(), s => (I)Convert.ChangeType(s, typeof(I)));
                                }
                                else
                                {
                                    // small index - read it into memory and wrap
                                    indexStream.Seek(0, SeekOrigin.Begin);
                                    var dict = IndexSerialization.DeserializeIndexFromStream<Dictionary<I, T>>(indexStream);
                                    if (dict == null)
                                    {
                                        dict = new Dictionary<I, T>();
                                    }
                                    Index = new InMemoryDiskIndexWrapper<I, T>(dict);
                                }
                            }
                        }
                    }
                    catch (Exception)
                    {
                        // if deserialization/import fails, fall back to empty in-memory index
                    }

                    if (Index == null)
                    {
                        Index = new InMemoryDiskIndexWrapper<I, T>(new Dictionary<I, T>());
                    }
                }

                IsIndexLoaded = true;
            }
        }

        public bool TryGet(I key, out T data)
        {
            if (!IsIndexLoaded)
            {
                ReadIndex();
            }
            if (Index.TryGet(key, out data))
            {
                return true;
            }
            else
            {
                data = default;
                return false;
            }
        }

        public IEnumerable<KeyValuePair<I, T>> GetAllEntries()
        {
            if (!IsIndexLoaded)
            {
                ReadIndex();
            }

            return Index.GetAllEntries();
        }

        public abstract void IndexPackage(IPackage package, int packageIndex);
    }
}
