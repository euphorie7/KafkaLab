// Pipeline CI/CD Jenkins pour le projet Kafka-lab
// Cette pipeline est destinée à la branche "delivery"
pipeline {
    agent any // executer sur n'importe quel agent

    options {
        // on ajouter l'heure aux logs bien sur , question de debugger
        timestamps()

        // empecher les executions simultanees
        disableConcurrentBuilds()

        // on disactive la recuperation automatique du code source 
        skipDefaultCheckout(true)
    }

    environment {
        // Url de mon depot git
        Git_REPO = 'https://github.com/euphorie7/KafkaLab.git'
        GIT_BRANCH = 'delivery'

        GIT_CREDENTIALS_ID = 'github-credentials'
        JFROG_CREDENTIALS_ID = 'jfrog-credentials'

        REGISTRY = 'localhost:5000'

        APP_NAME = 'kafkalab'


        COMMON_MODULE = 'kafka-commun'
        PRODUCER_MODULE = 'producer'
        CONSUMER_MODULE = 'consumer'

        // Noms des images Docker
        PRODUCER_IMAGE = 'image-producer'
        CONSUMER_IMAGE = 'image-consumer'

    }

    triggers {
        // Vérifier les changements chaque heure
        pollSCM('H * * * *')
    }

    stages {

        stage('git checkout') {

            steps {

                echo "Recuperation du code source de la branche ${GIT_BRANCH} ..."

                git branch : "${GIT_BRANCH}",
                    credentialsId: "${GIT_CREDENTIALS_ID}",
                    url: "${GIT_REPO}"
            }

        }

        stage ('Build Kafka-commun') {

            steps {

                echo "Compilation du module kafka-commun ..."

                dir("${COMMON_MODULE}") {

                    // installer le jar dans le depot local MAven (bibliotheque)
                    sh 'mvn -B clean install -DskiptTests ' 

                }

            }


        }

        stage('Build Kafka Producer') {

            steps {

                echo 'Compilation et packaging du producer...'

                dir("${PRODUCER_MODULE}") {

                    // Compiler et générer le JAR du producer
                    sh 'mvn -B clean package -DskipTests'
                }
            }
        }

        stage('Build Kafka Consumer') {

            steps {

                echo 'Compilation et packaging du consumer...'

                dir("${CONSUMER_MODULE}") {

                    // Compiler et générer le JAR du consumer
                    sh 'mvn -B clean package -DskipTests'
                }
            }
        }

        stage('Docker Build Producer') {

            steps {

                echo 'Création de l’image Docker Producer...'

                dir("${PRODUCER_MODULE}") { 

                    sh '''
                        docker build \
                            -t ${PRODUCER_IMAGE}:latest\
                            .
                    '''
                }

            }
        }

        stage('Docker Build Consumer') {

            steps {

                echo 'Création de l’image Docker Consumer...'

                dir("${CONSUMER_MODULE}") {

                    // Construire l'image avec le Dockerfile existant
                    sh '''
                        docker build \
                            -t ${CONSUMER_IMAGE}:latest \
                            .
                    '''
                }
            }

        }

        stage('Push Docker Images') {
            steps {

                sh '''

                    set -eu

                    VERSION="v${BUILD_NUMBER}"

                    BASE_PATH="${REGISTRY}/${APP_NAME}/${VERSION}"

                    docker tag "${PRODUCER_IMAGE}":latest \
                    "${BASE_PATH}/producer:${VERSION}"

                    docker tag "${CONSUMER_IMAGE}":latest \
                    "${BASE_PATH}/producer:${VERSION}" 

                    docker push "${BASE_PATH}/consumer:${VERSION}"
                    docker push "${BASE_PATH}/consumer:${VERSION}"


                '''

            }
        }

    }

    post {

        success {
            echo 'SUCCESS : Build Kafka terminé !'
            echo "Images créées : ${PRODUCER_IMAGE} et ${CONSUMER_IMAGE}"
        }

        failure {
            echo 'FAILED : Une étape de la pipeline a échoué.'
        }

        always {
            echo 'Fin de la pipeline CI/CD Kafka.'
        }

    }

}