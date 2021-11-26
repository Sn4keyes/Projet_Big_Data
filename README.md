# Projet_Big_Data

# Mise en place du docker:

Lancez Docker.
Ouvrez un terminal et rendez vous dans le dossier du projet.
Lorsque vous êtes dans le dossier 'Projet_Big_Data', faites un: 

docker-compose up --build

Ouvrez un autre terminal (qui sera le terminal de votre docker), rendez vous dans le dossier du projet et lancez un:

docker exec -it pyspark_notebook bash

Une fois dans le docker faites un cd ../work pour entrer dans le bon fichier work dans lequel se trouve notre consumer (consumer.py), vous remarquerez qu'il n'y a pas le producer qui lui se trouve sur la machine hors du docker.

Dans le docker exécutez le fichier consumer.py:

python consumer.py

Ce dernier va se lancer et sera sur écoute (après un cours instant). Une fois qu'il est sur écoute lancez le producer sur un autre terminal (en local):

python producer.py

Il met quelques temps à s'exécuter mais une fois les données envoyées au consumer, vous pourrez observez les résultats s'afficher sur le terminal de votre docker.

# Analyser les statistiques présentées

Nous affichons des moyennes, des max/min ainsi que des facteurs de corrélation.

Concernant les facteurs de corrélation nous avons choisit d'utiliser la corrélation de Pearson car nous avons supposé que la corrélation entre nos données serait linéaire. Le facteur de corrélation se trouve entre [-1; 1], plus sa valeur absolue est proche de 1, plus la corrélation entre les données comparées est forte (exemple de la corrélation en le prix du bitcoin et lui même = 1). Le signe quant à lui indique si la corrélation se fait 'dans le même sens' ou pas; en effet si la corrélation entre le bitcoin et l'ethereum est positive, cela signifie que si le prix du bitcoin augmente, celui de l'ethereum aura tendence à augmenter aussi tandis que si la corrélation était négative le prix de l'ethereum aurait plutôt tendence à baisser quand celui du bitcoin augmente.