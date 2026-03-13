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
    def dockerRegistry = env.DOCKER_REGISTRY_INTERNAL

    stage("Build and Push Docker Image") {
        docker.withRegistry("http://${dockerRegistry}", env.DOCKER_REGISTRY_CREDENTIALS_INTERNAL) {
            sh """
                make container \
                    REGISTRY=${dockerRegistry}/catalogicsoftware \
                    VERSION=${imageTag} \
                    BUILDX_PLATFORMS=linux/amd64,linux/arm64 \
                    BUILDX_OUTPUT_TYPE=registry \
                    IMAGE_TAGS='${dockerRegistry}/catalogicsoftware/velero:${veleroBranch} ${dockerRegistry}/catalogicsoftware/velero:${imageTag}'
            """
        }
    }
}
