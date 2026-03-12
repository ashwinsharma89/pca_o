#!/bin/bash
# Service management script for EC2

EC2_HOST="13.200.71.62"
EC2_USER="ubuntu"

show_usage() {
    echo "Usage: $0 {start|stop|restart|status|logs} [service_name]"
    echo ""
    echo "Commands:"
    echo "  start [service]   - Start all services or specific service"
    echo "  stop [service]    - Stop all services or specific service"
    echo "  restart [service] - Restart all services or specific service"
    echo "  status            - Show status of all services"
    echo "  logs [service]    - Show logs (default: all services)"
    echo ""
    echo "Services:"
    echo "  pca-api, celery-worker, celery-beat, flower, jaeger, redis, db"
    echo ""
    echo "Examples:"
    echo "  $0 status"
    echo "  $0 restart pca-api"
    echo "  $0 logs celery-worker"
}

if [ $# -eq 0 ]; then
    show_usage
    exit 1
fi

COMMAND=$1
SERVICE=${2:-}

case $COMMAND in
    start)
        if [ -z "$SERVICE" ]; then
            echo "🚀 Starting all services..."
            ssh ${EC2_USER}@${EC2_HOST} 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml up -d'
        else
            echo "🚀 Starting $SERVICE..."
            ssh ${EC2_USER}@${EC2_HOST} "cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml up -d $SERVICE"
        fi
        ;;
    
    stop)
        if [ -z "$SERVICE" ]; then
            echo "🛑 Stopping all services..."
            ssh ${EC2_USER}@${EC2_HOST} 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml down'
        else
            echo "🛑 Stopping $SERVICE..."
            ssh ${EC2_USER}@${EC2_HOST} "cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml stop $SERVICE"
        fi
        ;;
    
    restart)
        if [ -z "$SERVICE" ]; then
            echo "🔄 Restarting all services..."
            ssh ${EC2_USER}@${EC2_HOST} 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml restart'
        else
            echo "🔄 Restarting $SERVICE..."
            ssh ${EC2_USER}@${EC2_HOST} "cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml restart $SERVICE"
        fi
        ;;
    
    status)
        echo "📊 Service Status:"
        ssh ${EC2_USER}@${EC2_HOST} 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml ps'
        ;;
    
    logs)
        if [ -z "$SERVICE" ]; then
            echo "📋 Showing logs for all services (last 50 lines)..."
            ssh ${EC2_USER}@${EC2_HOST} 'cd /home/ubuntu/pca_o/docker && docker compose -f docker-compose.full.yml logs --tail=50'
        else
            echo "📋 Showing logs for $SERVICE (last 50 lines)..."
            ssh ${EC2_USER}@${EC2_HOST} "docker logs $SERVICE --tail 50 -f"
        fi
        ;;
    
    *)
        echo "❌ Unknown command: $COMMAND"
        show_usage
        exit 1
        ;;
esac
