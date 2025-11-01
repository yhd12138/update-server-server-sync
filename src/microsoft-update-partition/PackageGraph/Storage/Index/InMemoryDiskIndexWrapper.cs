using System;
using System.Collections.Generic;

namespace Microsoft.PackageGraph.Storage.Index
{
    public class InMemoryDiskIndexWrapper<I, T> : IDiskIndex<I, T>
    {
        private readonly Dictionary<I, T> _dict;

        public InMemoryDiskIndexWrapper(Dictionary<I, T> dict)
        {
            _dict = dict ?? new Dictionary<I, T>();
        }

        public void Add(I key, T value)
        {
            _dict[key] = value;
        }

        public bool TryGet(I key, out T value)
        {
            return _dict.TryGetValue(key, out value);
        }

        public IEnumerable<KeyValuePair<I, T>> GetAllEntries()
        {
            foreach (var kv in _dict)
            {
                yield return kv;
            }
        }

        public void Dispose()
        {
            // nothing to dispose for in-memory
        }
    }
}
