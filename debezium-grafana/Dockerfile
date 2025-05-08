FROM grafana/grafana:9.4.7

COPY dashboard.yml /etc/grafana/provisioning/dashboards
COPY datasource.yml /etc/grafana/provisioning/datasources
COPY debezium-dashboard.json /var/lib/grafana/dashboards/debezium-dashboard.json
