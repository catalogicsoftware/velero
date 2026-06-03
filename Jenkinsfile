def buildParamPrefix = "BUILD_"
properties([
    parameters([
        booleanParam(
            name: "${buildParamPrefix}VELERO",
            defaultValue: true,
            description: "Build and push base velero image"
        ),
        booleanParam(
            name: "${buildParamPrefix}CLOUDCASA_VELERO",
            defaultValue: true,
            description: "Build and push assembled cloudcasa-velero image"
        ),
        string(
            name: "VELEROPLUGIN_IMAGE_NAME",
            defaultValue: "amds-veleroplugin",
            description: "Plugin image name used in plugins.ini for cloudcasa-velero"
        ),
        string(
            name: "VELEROPLUGIN_VERSION",
            defaultValue: "",
            description: "Optional explicit plugin version. If empty, use committed plugins.ini plugin version"
        )
    ])
])

node("cloudcasa-build") {
    stage("Checkout") {
        cleanWs()
        checkout scm
    }

    def sourceBranch = env.BRANCH_NAME ?: "unknown"
    def branchTag = sourceBranch.replaceAll('[^0-9A-Za-z-]', '-')
    def masterEquivalentBranches = ["master", "v1.14.0.x", "jg-KUBEDR-7845"]
    def isMasterFlow = masterEquivalentBranches.contains(sourceBranch)

    // Keep tags cloudcasa-like while preserving velero version context.
    def baseVersion = "1.14.0"
    def veleroTag = "v${baseVersion}-${branchTag}.${env.BUILD_NUMBER}"
    def cloudcasaVeleroTag = "${baseVersion}-${branchTag}.${env.BUILD_NUMBER}"

    def dockerRegistryInternal = env.DOCKER_REGISTRY_INTERNAL
    def dockerRegistryCredsInternal = env.DOCKER_REGISTRY_CREDENTIALS_INTERNAL
    def dockerPrefixInternal = "${dockerRegistryInternal}/catalogicsoftware"

    def buildVelero = (params["${buildParamPrefix}VELERO"] ?: false) && isMasterFlow
    def buildCloudcasaVelero = (params["${buildParamPrefix}CLOUDCASA_VELERO"] ?: false) && isMasterFlow

    stage("Build velero image") {
        if (buildVelero) {
            env.BUILDX_CONFIG = "${env.HOME}/.docker/buildx"
            docker.withRegistry("https://${dockerRegistryInternal}", dockerRegistryCredsInternal) {
                sh """
                    set -eu
                    make container \
                        REGISTRY=${dockerPrefixInternal} \
                        VERSION=${veleroTag} \
                        BUILDX_PLATFORMS=linux/amd64,linux/arm64 \
                        BUILDX_OUTPUT_TYPE=registry \
                        IMAGE_TAGS='${dockerPrefixInternal}/velero:${veleroTag}'
                """
            }
        }
    }

    stage("Build cloudcasa-velero image") {
        if (buildCloudcasaVelero) {
            env.BUILDX_CONFIG = "${env.HOME}/.docker/buildx"

            def pluginImageName = (params.VELEROPLUGIN_IMAGE_NAME ?: "amds-veleroplugin").trim()
            def pluginVersion = (params.VELEROPLUGIN_VERSION ?: "").trim()

            // Keep the config deterministic: use this build's velero image tag.
            def veleroBaseTagForCloudcasa = veleroTag
            sh """
                set -eu
                awk '
                    BEGIN { in_velero = 0 }
                    /^\[velero\]$/ { in_velero = 1; print; next }
                    /^\[/ { in_velero = 0 }
                    in_velero && /^image[[:space:]]*=/ {
                        print "image = ${dockerPrefixInternal}/velero:${veleroBaseTagForCloudcasa}"
                        next
                    }
                    { print }
                ' plugins.ini > plugins.ini.tmp
                mv plugins.ini.tmp plugins.ini
            """

            if (pluginVersion) {
                sh """
                    set -eu
                    awk '
                        BEGIN { in_amds = 0 }
                        /^\[plugin:amds\]$/ { in_amds = 1; print; next }
                        /^\[/ { in_amds = 0 }
                        in_amds && /^image[[:space:]]*=/ {
                            print "image = catalogicsoftware/${pluginImageName}:${pluginVersion}"
                            next
                        }
                        { print }
                    ' plugins.ini > plugins.ini.tmp
                    mv plugins.ini.tmp plugins.ini
                """
            }

            docker.withRegistry("https://${dockerRegistryInternal}", dockerRegistryCredsInternal) {
                sh """
                    set -eu
                    bash build-plugins.sh \
                        --config plugins.ini \
                        ${dockerPrefixInternal}/cloudcasa-velero:${cloudcasaVeleroTag} \
                        --platform linux/amd64,linux/arm64 \
                        --push
                """
            }

            archiveArtifacts artifacts: "plugins.ini", onlyIfSuccessful: true
        }
    }
}
