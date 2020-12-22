using System;
using System.Threading.Tasks;

namespace Surfrider.Jobs {
    public interface IDataFileWriter
    {
        Task UpdateJsonFileWithDataAsync(int contributors, int coveredKm, int trashPerKm);
    }
}