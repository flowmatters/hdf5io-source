using System.Collections.Generic;

namespace FlowMatters.Source.HDF5IO
{
    public class UniqueNameResolver
    {
        HashSet<string> keysUsed = new HashSet<string>();

        public string UniquePath(string path)
        {
            path = H5SafeName(path);

            string origPath = path;
            int suffix = 1;
            while (keysUsed.Contains(path))
            {
                path = $"{origPath} {suffix}";
                suffix++;
            }
            keysUsed.Add(path);
            return path;
        }

        public static string H5SafeName(string path)
        {
            return path.Replace("/", Constants.SLASH_SUBST);
        }

        public static string RestoreName(string name)
        {
            return name.Replace(Constants.SLASH_SUBST, "/");
        }

        public void Reset()
        {
            keysUsed.Clear();
        }
    }
}
