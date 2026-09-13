# start-cluster.ps1 - Phase 7 helper script (not part of the Java project
# itself - just a convenience so you don't have to open 9 terminal windows
# and type 9 nearly-identical commands by hand every time you want to test
# the Raft cluster).
#
# WHAT THIS DOES: for each of the 9 node config files, opens a BRAND NEW
# PowerShell window and runs `java -jar target/redis-server.jar <config>`
# in it. Each node gets its own window so you can watch its log output
# separately and tell at a glance which one becomes leader for each shard.
#
# Start-Process launches a new, independent process (a new PowerShell
# window, in this case) without waiting for it to finish - exactly what we
# want here, since each of these 9 server processes runs forever until you
# close its window or press Ctrl+C in it.
#
# IMPORTANT: run this from the project's root folder (the same folder
# start-cluster.ps1 itself sits in) - it uses relative paths
# (target/redis-server.jar, config/nodeX.properties) that only resolve
# correctly from there. If you see "jarfile not found" errors, check your
# current folder first with `pwd`.

$nodes = @(
    "node1", "node1b", "node1c",
    "node2", "node2b", "node2c",
    "node3", "node3b", "node3c"
)

foreach ($node in $nodes) {
    $configPath = "config/$node.properties"
    Write-Host "Starting $node using $configPath ..."
    Start-Process powershell -ArgumentList "-NoExit", "-Command", "java -jar target/redis-server.jar $configPath"
    # A tiny stagger between launches - purely cosmetic (so the 9 windows
    # don't all pop up in one confusing instant), not required for
    # correctness. Raft's own randomized election timeouts already handle
    # nodes starting at genuinely different times without any issue.
    Start-Sleep -Milliseconds 300
}

Write-Host ""
Write-Host "All 9 nodes launched in separate windows."
Write-Host "Watch for 'Became LEADER for term N' in exactly ONE window per shard"
Write-Host "(node1/node1b/node1c is shard 1, node2/node2b/node2c is shard 2, node3/node3b/node3c is shard 3)."
