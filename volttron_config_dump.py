import subprocess

#BUG - First Pipe Output is shown.  Not critical.
status_cmd=subprocess.check_output(["volttron-ctl","status"],universal_newlines=True)
status_lines=status_cmd.split("\n")
for status_line in status_lines:
  status_array=status_line.split()
  if len(status_array)>=2:
    vip=status_array[2]
    print vip
    clist_cmd=subprocess.check_output(["volttron-ctl","config","list",vip],universal_newlines=True)
    clist_lines=clist_cmd.split("\n")
    for clist_line in clist_lines[0:-1]:
        print "..", clist_line
        get_cmd=subprocess.check_output(["volttron-ctl","config","get",vip,clist_line],universal_newlines=True)
	print get_cmd

        

