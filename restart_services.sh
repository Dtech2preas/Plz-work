#!/bin/bash

# Kill existing services
echo "Stopping existing services..."
pkill -f "cloudflared tunnel run"
pkill -f "python3 ser.py"
pkill -f "python3 app.py"

# Force kill any processes on the ports we need
echo "Freeing up ports 5000 and 5001..."
fuser -k 5000/tcp 2>/dev/null
fuser -k 5001/tcp 2>/dev/null

# Additional cleanup for stubborn processes
sudo lsof -ti:5000 | xargs kill -9 2>/dev/null
sudo lsof -ti:5001 | xargs kill -9 2>/dev/null

sleep 3

# Check if ports are clear
echo "Checking port availability..."
if lsof -Pi :5000 -sTCP:LISTEN -t >/dev/null; then
    echo "❌ WARNING: Port 5000 still in use"
    echo "Processes using port 5000:"
    lsof -i :5000
fi

if lsof -Pi :5001 -sTCP:LISTEN -t >/dev/null; then
    echo "❌ WARNING: Port 5001 still in use"
    echo "Processes using port 5001:"
    lsof -i :5001
fi

echo "Starting services..."
/home/PREASX/start_services.sh
