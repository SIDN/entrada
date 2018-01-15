pipeline {
    agent any 
    stages {
        stage('Build') { 
            steps {
                sh 'mvn -f dnslib4java/pom.xml -B -DskipTests clean package' 
                sh 'mvn -f pcaplib4java/pom.xml -B -DskipTests clean package'
                sh 'mvn -f pcap-to-parquet/pom.xml -B -DskipTests clean package'
                archiveArtifacts artifacts: 'pcap-to-parquet/target/**/*.jar', fingerprint: true, onlyIfSuccessful: true
            }
        }
    }
    post {
        always {
           /* junit 'src/crawler/target/surefire-reports/*.xml' */
           sh "echo 'Done'"
        }
        failure {
           emailext body: "Something is wrong with ${env.BUILD_URL}", recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'CulpritsRecipientProvider']], subject: "Failed Pipeline: ${currentBuild.fullDisplayName}"
        }
    }
}
