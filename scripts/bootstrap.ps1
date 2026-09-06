$ErrorActionPreference = "Stop"

$repoRoot = Split-Path -Parent $PSScriptRoot
Set-Location $repoRoot

$envFiles = @(
    @{
        Path = Join-Path $repoRoot ".env"
        Example = Join-Path $repoRoot ".env.example"
    },
    @{
        Path = Join-Path $repoRoot "disciple-registry/.env"
        Example = Join-Path $repoRoot "disciple-registry/.env.example"
    },
    @{
        Path = Join-Path $repoRoot "resource-allocation/.env"
        Example = Join-Path $repoRoot "resource-allocation/.env.example"
    }
)

foreach ($envFileset in $envFiles) {
    if (-not (Test-Path $envFileset.Path)) {
        Copy-Item $envFileset.Example $envFileset.Path
        Write-Host "Created $($envFileset.Path) from $($envFileset.Example)"
    }
}

Write-Host "Starting local infrastructure..."
docker compose up -d

function Wait-ContainerHealthy {
    param(
        [Parameter(Mandatory = $true)]
        [string]$ContainerName,
        [int]$TimeoutSeconds = 120
    )

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)

    while ((Get-Date) -lt $deadline) {
        $status = docker inspect --format "{{.State.Health.Status}}" $ContainerName 2>$null

        if ($LASTEXITCODE -eq 0 -and $status.Trim() -eq "healthy") {
            return
        }

        Start-Sleep -Seconds 2
    }

    docker compose ps
    throw "Container '$ContainerName' did not become healthy within $TimeoutSeconds seconds."
}

Wait-ContainerHealthy "cloud-stream-postgres"
Wait-ContainerHealthy "cloud-stream-kafka"

$kafkaTopics = @(
    @(
        "disciple-registry.cultivation-advanced.v1",
        "3"
    ),
    @(
        "resource-allocation.cultivation-advanced.dlq.v1",
        "1"
    )
)

foreach ($topic in $kafkaTopics) {
    $topicName = $topic[0]
    $partitionCount = $topic[1]

    docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
        --bootstrap-server localhost:29092 `
        --create `
        --if-not-exists `
        --topic $topicName `
        --partitions $partitionCount `
        --replication-factor 1
}

Write-Host ""
Write-Host "Local infrastructure is ready."
docker compose ps

Write-Host ""
Write-Host "Kafka topics:"
docker compose exec -T kafka /opt/kafka/bin/kafka-topics.sh `
    --bootstrap-server localhost:29092 `
    --list

Write-Host ""
Write-Host "Run the Go API and workers manually from their module directories."
