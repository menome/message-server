pipeline {
  agent any
  stages {
    stage('test') {
      tools {
            jdk "jdk-11.0.7"
      }
      steps {
        sh 'java -version'
        sh './gradlew clean assemble test --tests "com.menome.*"'
      }
    }
  }
  post {
      always {
          junit 'build/**/*.xml'
      }
  }
}
