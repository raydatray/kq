#!/bin/zsh
# usage: run.sh LABEL RPS DUR PARTITIONS WORKER_PROCS WORKERS_PER_PROC CONCURRENCY [PRODUCER_CLIENTS] [GOROUTINES]
# env:   EXEC=northstar|const:<ms>  OUT=<dir>
set -u
HERE=${0:A:h}
REPO=${HERE:h:h:h:h:h}
LABEL=$1 RPS=$2 DUR=$3 PART=$4 WP=$5 WPP=$6 C=$7 PC=${8:-2} PG=${9:-512} EXEC=${EXEC:-northstar}
D=${OUT:-${TMPDIR:-/tmp}/kq-probe}/$LABEL; rm -rf $D; mkdir -p $D
BIN=$D/probe
(cd $REPO && go build -o $BIN $HERE/main.go)
KC=(docker compose -f $REPO/bench/compose.yaml -p kqprobe)
$KC up -d --wait >/dev/null 2>&1
$KC exec -T kafka /opt/kafka/bin/kafka-topics.sh --bootstrap-server localhost:19092 --create --topic probe-ready --partitions $PART --replication-factor 1 >/dev/null
$KC exec -T kafka /opt/kafka/bin/kafka-configs.sh --bootstrap-server localhost:19092 --alter --entity-type groups --entity-name kq.probe.workers --add-config share.auto.offset.reset=earliest >/dev/null
for i in $(seq 0 $((WP-1))); do
  $BIN work -workers $WPP -concurrency $C -exec $EXEC -out $D/ids-$i.bin > $D/work-$i.out 2> $D/work-$i.err &
done
sleep 12 # let share group assignment settle
$BIN produce -clients $PC -goroutines $PG -rps $RPS -dur ${DUR}s > $D/produce.out 2>&1
wait
echo "== $LABEL rps=$RPS dur=$DUR part=$PART procs=$WP x workers=$WPP x C=$C exec=$EXEC"
cat $D/produce.out $D/work-*.out
$BIN check $D/ids-*.bin
$KC down -v >/dev/null 2>&1
