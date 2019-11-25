##  Author: Queena.xie@sap.com
if [ $# -ne 1 ]
then 
	echo "usage: ./$0 <gstack file name>"
	exit
fi
if [ -f $1 ] 
then
cat  $1 |awk 'BEGIN {  current_thread=0 
			}
{
		if ($0 ~"^Thread ")
		{
			split($NF,a,")")
			data[$2]=a[1]" "
			current_thread=$2
		}
		else if ($0~"^#")
			{
				data[current_thread]=data[current_thread]" "$0
			}
}
END{ 
		for (i in data)
			print i,data[i]
}'| awk '{
		s=""
		for(g=3;g<=NF;g++)
			s=s""$g

		output[s]=output[s]" "$1"("$2"),"
		count[s]++;
	}END{
		for (string in output)
		{	
			print "Same callstack is "count[string]
			print output[string]
			n=split(string,ss,"#");
			for(r=2;r<=n;r++)
			{
				print "#"ss[r]
			}
		}
}'>tmp.txt
cat tmp.txt
cat tmp.txt|grep "Same callstack is" |sort -n -t ' ' -k4
cat tmp.txt|awk 'BEGIN{sum=0;}{
		if($0~"Same callstack is")
		{
			sum=sum+$4
		}
			}END{print "";print "Totally call stack number is:"sum}' 
fi
