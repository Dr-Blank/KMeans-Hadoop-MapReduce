if (-NOT ([Security.Principal.WindowsPrincipal][Security.Principal.WindowsIdentity]::GetCurrent()).IsInRole([Security.Principal.WindowsBuiltInRole] "Administrator")) {  
  $arguments = "& '" + $myinvocation.mycommand.definition + "'"
  Start-Process powershell -Verb runAs -ArgumentList $arguments
  Break
}

Invoke-Item $env:HADOOP_HOME\sbin\start-dfs.cmd
Invoke-Item $env:HADOOP_HOME\sbin\start-yarn.cmd
