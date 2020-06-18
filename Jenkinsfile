pipeline {
    agent any
    stages {
        stage('Build and Test') {
            tools {
                jdk "jdk-11.0.7"
            }
            steps {
                sh 'java -version'
                sh './gradlew clean assemble test --tests "com.menome.*"'
            }
        }
        stage("Build and publish Docker Image") {
            steps {
                script {
                    docker.withRegistry('https://registry.hub.docker.com', 'docker-hub-toddcostella') {

                        def customImage = docker.build("menome/message-server")
                        customImage.push("version-${env.BUILD_NUMBER}")
                        customImage.push("latest")
                    }
                }
            }
        }
    }

    post {
        always {
            junit 'build/**/*.xml'
            mattermostSend channel: "#80 Symend Alerts", message: "Testing"
        }
    }
}