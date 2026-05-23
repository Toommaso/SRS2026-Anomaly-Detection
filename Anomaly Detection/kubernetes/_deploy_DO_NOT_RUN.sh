

# storage
# 1. Installa il provisioner
kubectl apply -f https://raw.githubusercontent.com/rancher/local-path-provisioner/v0.0.36/deploy/local-path-storage.yaml

# 2. Verifica che sia in Running
kubectl get pods -n local-path-storage

# 3. Verifica che la StorageClass esista
kubectl get storageclass local-path



# mongo (percona operator)
helm install psmdb-operator psmdb-operator \
  --namespace factory \
  --repo https://percona.github.io/percona-helm-charts/ \
  -f mongodb-operator-values.yaml

# attendi deployment
kubectl rollout status deployment/psmdb-operator -n factory

# mongo (cluster)
helm install mongodb psmdb-db \
  --namespace factory \
  --repo https://percona.github.io/percona-helm-charts/ \
  -f mongodb-values.yaml

# Monitora la creazione (può richiedere 3-5 minuti)
kubectl get psmdb -n factory -w

# Visualizza tutti gli utenti generati
kubectl get secret mongodb-secrets -n mongodb -o json | \
  jq -r '.data | to_entries[] | "\(.key): \(.value | @base64d)"'

#connection string
#mongodb://userAdmin:<password>@mongodb-mongos.mongodb.svc.cluster.local:27017/admin
# kubectl exec -it mongodb-psmdb-db-rs0-0 -n factory -- mongosh


#docker hub login
# 1. Crea il secret con le credenziali Docker Hub
kubectl create secret docker-registry dockerhub-secret \
  --docker-server=docker.io \
  --docker-username=<tuo-username> \
  --docker-password=<tua-password> \
  --namespace factory

# 2. Aggiungilo al service account default
kubectl patch serviceaccount default -n factory \
  -p '{"imagePullSecrets": [{"name": "dockerhub-secret"}]}'







# kafka

# STEP 1: installa l'operator (da quay.io, nessun rate limit)
helm install strimzi-operator \
  oci://quay.io/strimzi-helm/strimzi-kafka-operator \
  --namespace factory \
  -f kafka-operator-values.yaml

# Aspetta che l'operator sia pronto
kubectl rollout status deployment/strimzi-cluster-operator -n factory

# STEP 2: crea il cluster Kafka
kubectl apply -f kafka-cluster.yaml -n factory

# Monitora
kubectl get kafka -n factory -w


kubectl get pvc -n factory

kubectl describe nodes | grep -A 8 "Allocated resources:"


# mqtt (vecchio)
#helm install mosquitto \
#  oci://ghcr.io/helmforgedev/helm/mosquitto \
#  --namespace factory \
#  -f mosquitto-values.yaml

#mqtt v2
helm install emqx emqx \
  --namespace factory \
  --repo https://repos.emqx.io/charts \
  -f emqx-values.yaml


# 1. nginx ingress controller

helm install ingress-nginx ingress-nginx \
  --namespace ingress-nginx \
  --create-namespace \
  --repo https://kubernetes.github.io/ingress-nginx \
  -f ingress-nginx-values.yaml

# Aspetta l'IP pubblico OCI (1-2 minuti)
kubectl get svc -n ingress-nginx -w
# quando compare EXTERNAL-IP, annotalo

# 2. mongo-express
helm install mongo-express mongo-express \
  --namespace factory \
  --repo https://cowboysysop.github.io/charts/ \
  -f mongo-express-values.yaml

# 3. kafka-ui
helm install kafka-ui kafka-ui \
  --namespace factory \
  --repo https://ui.charts.kafbat.io \
  -f kafka-ui-values.yaml


## pwd ingress
# Installa htpasswd se non ce l'hai (su mac: brew install httpd)
htpasswd -c auth admin
# ti chiede la password, inseriscila

# Crea il Secret nel namespace factory
kubectl create secret generic basic-auth \
  --from-file=auth \
  --namespace factory

# in /etc/hosts
# 130.61.84.187 kafka.local
# 130.61.84.187 mongo.local
# add
sudo bash -c '
echo "130.61.84.187 kafka.local" >> /etc/hosts
echo "130.61.84.187 grafana.local" >> /etc/hosts
echo "130.61.84.187 mongo-express.local" >> /etc/hosts
echo "130.61.84.187 headlamp.local" >> /etc/hosts
'
# rm
sudo bash -c '
sed -i "/130.61.84.187 kafka.local/d" /etc/hosts
sed -i "/130.61.84.187 grafana.local/d" /etc/hosts
sed -i "/130.61.84.187 mongo-express.local/d" /etc/hosts
sed -i "/130.61.84.187 headlamp.local/d" >> /etc/hosts
'

# see hosts
kubectl get ingress -n factory

# logs
kubectl logs -n factory mongo-express-646c896888-dvx7d

helm install headlamp headlamp \
  --repo https://kubernetes-sigs.github.io/headlamp/ \
  --namespace factory \
  --values headlamp-values.yaml


#token
kubectl create token headlamp -n factory --duration=1h

#metrics-server
kubectl apply -f https://github.com/kubernetes-sigs/metrics-server/releases/latest/download/components.yaml