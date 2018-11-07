pipeline {
    agent any 
    stages {
        stage('Build') { 
            steps {
                sh 'rm -rf entrada-*.tar.gz'
                sh 'mvn -f dnslib4java/pom.xml -B -DskipTests clean install' 
                sh 'mvn -f pcaplib4java/pom.xml -B -DskipTests clean install'
                sh 'mvn -f pcap-to-parquet/pom.xml -B -DskipTests clean package'
                sh './create_package.sh'
                archiveArtifacts artifacts: '*.tar.gz', fingerprint: true, onlyIfSuccessful: true
                archiveArtifacts artifacts: 'pcap-to-parquet/target/**/*.jar', fingerprint: true, onlyIfSuccessful: true
            }
        }
        stage('Test') {
            steps {
                sh 'mvn -f pcap-to-parquet/pom.xml clean clover:setup test clover:aggregate clover:clover'
                step([
                $class: 'CloverPublisher',
                cloverReportDir: 'pcap-to-parquet/target/site',
                cloverReportFileName: 'clover.xml',
                healthyTarget: [methodCoverage: 70, conditionalCoverage: 80, statementCoverage: 80], 
                unhealthyTarget: [methodCoverage: 50, conditionalCoverage: 50, statementCoverage: 50],
                failingTarget: [methodCoverage: 0, conditionalCoverage: 0, statementCoverage: 0]    
              ])
            }
         }
    }
    post {
        always {
           junit 'pcap-to-parquet/target/surefire-reports/*.xml'
           sh "echo 'Done'"
        }
        failure {
           emailext body: "Something is wrong with ${env.BUILD_URL}", recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'CulpritsRecipientProvider']], subject: "Failed Pipeline: ${currentBuild.fullDisplayName}"
        }
    }
}
