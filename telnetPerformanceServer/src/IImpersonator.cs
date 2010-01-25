using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PerformanceTools.Impersonation
{
    public interface IImpersonator : IDisposable
    {
        void Impersonate();
    }
    public class NullImpersonator : IImpersonator
    {
       public void Dispose() { }
       public void Impersonate() { }
    }
}
