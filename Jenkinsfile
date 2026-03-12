node("cloudcasa-build") {
    stage("Checkout") {
        cleanWs()
        checkout scm
    }

    def veleroBranch
    switch(env.BRANCH_NAME) {
        case 'master':
            veleroBranch = 'production'; break
        case 'prod-sh':
            veleroBranch = 'prod-sh'; break
        default:
            veleroBranch = 'v1.14.0.x'
    }

    def imageTag = "${veleroBranch}-${env.BUILD_NUMBER}"
    def nexusRegistry = "cc-nexus.ad.catalogic.us:8083"

    stage("Build and Push Docker Image") {
        docker.withRegistry("https://${nexusRegistry}", "cc-nexus.ad.catalogic.us-docker-registry") {
            sh """
                make container \
                    REGISTRY=${nexusRegistry}/catalogicsoftware \
                    VERSION=${imageTag} \
                    BUILDX_PLATFORMS=linux/amd64,linux/arm64 \
                    BUILDX_OUTPUT_TYPE=registry \
                    IMAGE_TAGS='${nexusRegistry}/catalogicsoftware/velero:${veleroBranch} ${nexusRegistry}/catalogicsoftware/velero:${imageTag}'
            """
        }
    }
}
