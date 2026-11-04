#!/bin/bash
# End to end with a quorum loss: the real server + RabbitMQ + the 3-node cluster of docker-compose.cluster.yml. 200,000
# messages are published; once a few batches are in, the leader AND a follower are killed for 55 s, longer than the driver's
# 30 s transaction retry, so batches really fail and the server must requeue them. Then both come back and the stream must
# finish: every employee exactly once, queue empty, nothing reported as bad data.
#
# Needs: the cluster up (docker compose -f docker-compose.cluster.yml up -d --wait), a RabbitMQ container named by
# $RABBIT (default rmq-probe: docker run -d --name rmq-probe -e RABBITMQ_DEFAULT_USER=messageserver -e RABBITMQ_DEFAULT_PASS=messageserver
# -p 5672:5672 rabbitmq:management-alpine), build/libs/messageServer-1.0-all.jar (./gradlew shadowJar), port 8081 free.
# Usage: scripts/cluster-quorum-loss-e2e.sh [server-log-file]
# 2026-11-02 result: RESULT: PASS - 200,000 employees (200,000 distinct), 600,000 relationships, 1-2 batches requeued
# (varies by run), 0 bad data, queue 0/0.
SERVER_LOG=${1:-/tmp/cluster-e2e-server.log}
RABBIT=${RABBIT:-rmq-probe}
cd "$(dirname "$0")/.."
any() { for c in cluster-server1 cluster-server2 cluster-server3; do out=$(docker exec $c cypher-shell -a bolt://localhost:7687 -u neo4j -p password --format plain "$1" 2>/dev/null | tail -n +2 | tr -d '"'); [ -n "$out" ] && { echo "$out"; return; }; done; }
rmq() { docker exec $RABBIT rabbitmqctl list_queues name messages messages_unacknowledged 2>/dev/null | grep -E "^test_queue" | awk '{print $2"/"$3}'; }
docker exec $RABBIT rabbitmqctl purge_queue test_queue >/dev/null 2>&1
# All members must be up (an earlier run may have left some stopped), then start from an empty test graph. Employees go first,
# in batches: the three shared hubs (Office, Project, Team) end up with one relationship per employee, and deleting a hub that
# still has hundreds of thousands of them is one transaction that exceeds a cluster member's 1 GB heap ("memory pool out of
# memory"). Once the employees are gone the hubs are trivial.
for c in cluster-server1 cluster-server2 cluster-server3; do docker start $c >/dev/null 2>&1; done
for i in $(seq 1 60); do n=$(any "SHOW DATABASES YIELD name, currentStatus WHERE name='neo4j' AND currentStatus='online' RETURN count(*)" | tail -1); [ "$n" = "3" ] && break; sleep 3; done
any "MATCH (e:Employee) WHERE e.SourceSystem = 'test_framework' CALL (e) { DETACH DELETE e } IN TRANSACTIONS OF 1000 ROWS" >/dev/null
any "MATCH (n) WHERE n.SourceSystem = 'test_framework' CALL (n) { DETACH DELETE n } IN TRANSACTIONS OF 1000 ROWS" >/dev/null
LEFT=$(any "MATCH (n) WHERE n.SourceSystem = 'test_framework' RETURN count(n)" | tail -1)
[ "$LEFT" = "0" ] || { echo "cannot start: $LEFT test nodes left over"; echo "RESULT: FAIL"; echo FINISHED; exit 1; }
EXPECTED=200000
(NEO4J_HOST=neo4j://localhost NEO4J_BOLT_PORT=7687 ENABLE_TEST_ENDPOINTS=Y WRITER_THREADS=2 PARSER_THREADS=1 nohup java -jar build/libs/messageServer-1.0-all.jar > $SERVER_LOG 2>&1 &)
for i in $(seq 1 40); do grep -q "Server Started" $SERVER_LOG && break; sleep 2; done
LEADER=$(any "SHOW DATABASES YIELD name, address, writer WHERE name='neo4j' AND writer RETURN address" | tail -1 | cut -d: -f2)
LEADER_C=cluster-server$((LEADER-7686)); FOLLOWER_C=$(for c in cluster-server1 cluster-server2 cluster-server3; do [ $c != $LEADER_C ] && echo $c; done | head -1)
START=$(date +%s)
curl -s -X POST localhost:8081/GenerateTestMessages/200000 | head -c 100; echo
echo "leader=$LEADER_C will be killed together with follower=$FOLLOWER_C"
KILLED=0; RESTORED=0
for i in $(seq 1 600); do
  DONE=$(grep -cE "Processed [0-9]+ messages" $SERVER_LOG)
  echo "t=$(( $(date +%s)-START ))s batches_done=$DONE queue(ready/unacked)=$(rmq) requeues=$(grep -c 'requeueing' $SERVER_LOG)"
  if [ "$KILLED" = 0 ] && [ "$DONE" -ge 8 ]; then
    echo ">>> QUORUM LOSS: killing $LEADER_C and $FOLLOWER_C at batch $DONE"; docker kill $LEADER_C $FOLLOWER_C >/dev/null; KILLED=$(date +%s)
  fi
  if [ "$KILLED" != 0 ] && [ "$RESTORED" = 0 ] && [ $(( $(date +%s)-KILLED )) -ge 55 ]; then echo ">>> restoring $LEADER_C $FOLLOWER_C after 55 s"; docker start $LEADER_C $FOLLOWER_C >/dev/null; RESTORED=1; fi
  if [ "$RESTORED" = 1 ] && [ "$(rmq)" = "0/0" ]; then break; fi
  if [ "$KILLED" = 0 ]; then sleep 0.3; else sleep 5; fi
done
echo "== final"
for i in $(seq 1 60); do n=$(any "SHOW DATABASES YIELD name, currentStatus WHERE name='neo4j' AND currentStatus='online' RETURN count(*)" | tail -1); [ "$n" = "3" ] && break; sleep 3; done
echo "members online: $n"
EMPLOYEES=$(any "MATCH (e:Employee) WHERE e.SourceSystem='test_framework' RETURN count(e), count(DISTINCT e.Email)" | tail -1)
RELS=$(any "MATCH (:Employee)-[r]->() WHERE r:LocatedInOffice OR r:WorkedOnProject OR r:HAS_FACET RETURN count(r)" | tail -1)
echo "employees (count, distinct): $EMPLOYEES   relationships: $RELS   (expected $EXPECTED employees, $((EXPECTED*3)) relationships)"
echo "queues: $(docker exec $RABBIT rabbitmqctl list_queues name messages messages_unacknowledged 2>/dev/null | grep -E '^test_queue' | tr '\t' ' ')"
echo "batches requeued by the server: $(grep -c 'requeueing' $SERVER_LOG)"
echo "messages reported as bad data: $(grep -E 'Processed [0-9]+ messages with [0-9]+ errors' $SERVER_LOG | sed -E 's/.*with ([0-9]+) errors.*/\1/' | paste -sd+ - | bc)"
grep -m3 "requeueing" $SERVER_LOG | cut -c1-300
pkill -f messageServer-1.0-all.jar
BAD=$(grep -E 'Processed [0-9]+ messages with [0-9]+ errors' $SERVER_LOG | sed -E 's/.*with ([0-9]+) errors.*/\1/' | paste -sd+ - | bc)
if [ "$EMPLOYEES" = "$EXPECTED, $EXPECTED" ] || [ "$EMPLOYEES" = "$EXPECTED,$EXPECTED" ] || [ "$(echo $EMPLOYEES | tr -d ' ,')" = "$EXPECTED$EXPECTED" ]; then EMP_OK=1; else EMP_OK=0; fi
if [ "$EMP_OK" = 1 ] && [ "$RELS" = "$((EXPECTED*3))" ] && [ "$BAD" = "0" ] && [ "$(rmq)" = "0/0" ] && [ "$n" = "3" ]; then echo "RESULT: PASS"; else echo "RESULT: FAIL"; fi
echo FINISHED
