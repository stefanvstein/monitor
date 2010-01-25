using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace PerformanceTools.Impersonation
{
    using System;
    using System.Security.Principal;
    using System.Runtime.InteropServices;
    using System.ComponentModel;
    public class ImpersonationException : ApplicationException {
        public ImpersonationException(String message)
            : base(message) { }
    }
    class Impersonator : IImpersonator
    {
        private String userName;
        private String domainName;
        private String password;

        public Impersonator(
            string userName,
            string domainName,
            string password)
        {
            this.userName=userName;
            this.domainName=domainName;
            this.password=password;
        }

        public void Impersonate(){
            ImpersonateValidUser(userName, domainName, password);
        }

        public void Dispose()
        {
            UndoImpersonation();
        }

        #region P/Invoke.

        [DllImport("advapi32.dll", SetLastError = true)]
        private static extern int LogonUser(
            string lpszUserName,
            string lpszDomain,
            string lpszPassword,
            int dwLogonType,
            int dwLogonProvider,
            ref IntPtr phToken);

        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern int DuplicateToken(
            IntPtr hToken,
            int impersonationLevel,
            ref IntPtr hNewToken);

        [DllImport("advapi32.dll", CharSet = CharSet.Auto, SetLastError = true)]
        private static extern bool RevertToSelf();

        [DllImport("kernel32.dll", CharSet = CharSet.Auto)]
        private static extern bool CloseHandle(
            IntPtr handle);

        private const int LOGON32_LOGON_INTERACTIVE = 2;
        private const int LOGON32_PROVIDER_DEFAULT = 0;

        // ------------------------------------------------------------------
        #endregion

        private void ImpersonateValidUser(
            string userName,
            string domain,
            string password)
        {
            WindowsIdentity tempWindowsIdentity = null;
            IntPtr token = IntPtr.Zero;
            IntPtr tokenDuplicate = IntPtr.Zero;

            try
            {
                if (RevertToSelf())
                {
                    if (LogonUser(
                        userName,
                        domain,
                        password,
                        LOGON32_LOGON_INTERACTIVE,
                        LOGON32_PROVIDER_DEFAULT,
                        ref token) != 0)
                    {
                        if (DuplicateToken(token, 2, ref tokenDuplicate) != 0)
                        {
                            tempWindowsIdentity = new WindowsIdentity(tokenDuplicate);
                            impersonationContext = tempWindowsIdentity.Impersonate();
                        }
                        else
                        {
                            throw new ImpersonationException( new Win32Exception(Marshal.GetLastWin32Error()).Message + ". While impersonating "+userName+ " at "+ domain);
                        }
                    }
                    else
                    {
                        throw new ImpersonationException(new Win32Exception(Marshal.GetLastWin32Error()).Message + ". While impersonating " + userName + " at " + domain);
                    }
                }
                else
                {
                    throw new ImpersonationException(new Win32Exception(Marshal.GetLastWin32Error()).Message + ". While impersonating " + userName + " at " + domain);
                }
            }
            finally
            {
                if (token != IntPtr.Zero)
                {
                    CloseHandle(token);
                }
                if (tokenDuplicate != IntPtr.Zero)
                {
                    CloseHandle(tokenDuplicate);
                }
            }
        }

        private void UndoImpersonation()
        {
            if (impersonationContext != null)
            {
                impersonationContext.Undo();
            }
        }

        private WindowsImpersonationContext impersonationContext = null;

    }
}
