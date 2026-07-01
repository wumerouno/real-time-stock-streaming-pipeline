param(
    [int]$EventRatePerSecond = 100,
    [switch]$RebuildDashboard,
    [switch]$SkipWait
)

$ErrorActionPreference = "Stop"

function Enable-DockerCli {
    if (Get-Command docker -ErrorAction SilentlyContinue) {
        return
    }

    $candidateDirs = @(
        "C:\Program Files\Docker\Docker\resources\bin",
        "C:\Program Files\Docker\Docker\resources"
    )

    foreach ($dir in $candidateDirs) {
        $dockerExe = Join-Path $dir "docker.exe"
        if (Test-Path $dockerExe) {
            $env:Path = "$dir;$env:Path"
            return
        }
    }

    throw "Docker CLI was not found. Start Docker Desktop and make sure docker.exe is on PATH."
}

function Invoke-Docker {
    param([string[]]$Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "docker $($Arguments -join ' ') failed with exit code $LASTEXITCODE"
    }
}

function Wait-HttpEndpoint {
    param(
        [string]$Name,
        [string]$Url,
        [int]$TimeoutSeconds = 120
    )

    if ($SkipWait) {
        return
    }

    $deadline = (Get-Date).AddSeconds($TimeoutSeconds)
    while ((Get-Date) -lt $deadline) {
        try {
            $response = Invoke-WebRequest -UseBasicParsing -Uri $Url -TimeoutSec 5
            if ($response.StatusCode -ge 200 -and $response.StatusCode -lt 500) {
                Write-Host "$Name is responding at $Url"
                return
            }
        } catch {
            Start-Sleep -Seconds 5
        }
    }

    Write-Warning "$Name did not respond at $Url within $TimeoutSeconds seconds."
}

function Test-SparkStreamingJob {
    $result = & docker exec stock-spark-master sh -lc "ps -ef | grep '[s]tock_streaming_job.py' >/dev/null && echo running || true"
    if ($LASTEXITCODE -ne 0) {
        throw "Could not inspect Spark streaming process."
    }

    return ($result -join "").Trim() -eq "running"
}

Enable-DockerCli

Write-Host "Starting Kafka, Kafka UI, and Spark cluster..."
Invoke-Docker -Arguments @("compose", "up", "-d", "zookeeper", "kafka", "kafka-ui", "spark-master", "spark-worker")

Write-Host "Creating Kafka topics..."
Invoke-Docker -Arguments @("compose", "run", "--rm", "topic-init")

Write-Host "Starting producer at $EventRatePerSecond events/sec..."
$env:EVENT_RATE_PER_SECOND = "$EventRatePerSecond"
Invoke-Docker -Arguments @("compose", "--profile", "runtime", "up", "-d", "producer")

Write-Host "Starting Streamlit dashboard..."
$dashboardArgs = @("compose", "--profile", "dashboard", "up", "-d")
if ($RebuildDashboard) {
    $dashboardArgs += "--build"
}
$dashboardArgs += "dashboard"
Invoke-Docker -Arguments $dashboardArgs

if (Test-SparkStreamingJob) {
    Write-Host "Spark streaming job is already running."
} else {
    Write-Host "Starting Spark streaming job in stock-spark-master..."
    $sparkCommand = "/opt/spark/bin/spark-submit --master spark://spark-master:7077 --conf spark.jars.ivy=/tmp/.ivy2 --packages org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.1,io.delta:delta-spark_2.12:3.2.0 /opt/stock-pipeline/spark/stock_streaming_job.py > /tmp/stock_streaming_job.log 2>&1"
    Invoke-Docker -Arguments @("exec", "-d", "stock-spark-master", "sh", "-lc", $sparkCommand)
}

Wait-HttpEndpoint -Name "Kafka UI" -Url "http://localhost:8080" -TimeoutSeconds 90
Wait-HttpEndpoint -Name "Spark Master UI" -Url "http://localhost:8081" -TimeoutSeconds 90
Wait-HttpEndpoint -Name "Streamlit dashboard" -Url "http://localhost:8501" -TimeoutSeconds 180

Write-Host ""
Write-Host "Local streaming demo is running."
Write-Host "Kafka UI:            http://localhost:8080"
Write-Host "Spark Master UI:     http://localhost:8081"
Write-Host "Streamlit dashboard: http://localhost:8501"
Write-Host ""
Write-Host "Useful log commands:"
Write-Host "  docker compose logs -f producer"
Write-Host "  docker exec stock-spark-master tail -f /tmp/stock_streaming_job.log"
Write-Host "  docker compose logs -f dashboard"
Write-Host ""
Write-Host "Stop everything with:"
Write-Host "  docker compose --profile runtime --profile dashboard down"
