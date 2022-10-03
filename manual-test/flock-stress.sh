set -eux

worker () {
	for i in $(seq 100)
	do
		flock foo.lock sh -c 'v=$(cat count) ; flock -n foo.lock true ; echo $((v+1)) > count'
	done
}

touch foo.lock
echo 0 > count

nworkers=10
for i in $(seq $nworkers)
do
	worker &
	timeout 10s abuse-posix-lock &
done

wait 

if test $(cat count) != $((100*$nworkers))
then
	echo "bad count!"
	exit 1
fi