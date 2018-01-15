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
        stage('Test') {
            steps {
                /*
                sh 'mvn -f src/crawler/pom.xml clean clover:setup test clover:aggregate clover:clover'
                step([
                $class: 'CloverPublisher',
                cloverReportDir: 'src/crawler/target/site',
                cloverReportFileName: 'clover.xml',
                healthyTarget: [methodCoverage: 70, conditionalCoverage: 80, statementCoverage: 80], 
                unhealthyTarget: [methodCoverage: 50, conditionalCoverage: 50, statementCoverage: 50],
                failingTarget: [methodCoverage: 0, conditionalCoverage: 0, statementCoverage: 0]    
              ])
              */
            }
         }
    }
    post {
        always {
           junit 'src/crawler/target/surefire-reports/*.xml'
        }
        failure {
           emailext body: "Something is wrong with ${env.BUILD_URL}", recipientProviders: [[$class: 'DevelopersRecipientProvider'], [$class: 'CulpritsRecipientProvider']], subject: "Failed Pipeline: ${currentBuild.fullDisplayName}"
        }
    }
}
