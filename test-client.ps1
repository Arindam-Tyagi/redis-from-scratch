# A tiny stand-in for redis-cli, using our server's plain-text "inline
# command" support (see RESPParser.parseInlineCommand) — no install needed.
# Usage: powershell -File test-client.ps1 -Port 6379

param(
    [int]$Port = 6379
)

$client = New-Object System.Net.Sockets.TcpClient("localhost", $Port)
$stream = $client.GetStream()
$writer = New-Object System.IO.StreamWriter($stream)
$writer.NewLine = "`r`n"
$writer.AutoFlush = $true
$reader = New-Object System.IO.StreamReader($stream)

Write-Host "Connected to localhost:$Port. Type commands (e.g. SET foo bar), or 'exit' to quit."

while ($true) {
    $line = Read-Host ">"
    if ($line -eq "exit") { break }
    $writer.WriteLine($line)

    # Wait for the FULL reply, not just whatever's arrived after a fixed
    # short sleep — SET/LPUSH/etc. involve a real disk fsync in the WAL,
    # which can take longer than a tiny fixed delay. So instead: keep
    # reading bytes as they arrive, and only stop once there's been a
    # short quiet gap (150ms with nothing new) AFTER we've received at
    # least something — that quiet gap is our signal "the reply is done."
    $response = New-Object System.Text.StringBuilder
    $deadline = (Get-Date).AddSeconds(3)
    $lastDataAt = $null

    while ((Get-Date) -lt $deadline) {
        if ($stream.DataAvailable) {
            [void]$response.Append([char]$reader.Read())
            $lastDataAt = Get-Date
        } elseif ($lastDataAt -ne $null -and ((Get-Date) - $lastDataAt).TotalMilliseconds -gt 150) {
            break
        } else {
            Start-Sleep -Milliseconds 20
        }
    }

    Write-Host $response.ToString()
}

$client.Close()
