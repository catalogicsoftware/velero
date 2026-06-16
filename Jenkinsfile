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
        booleanParam(
            name: "PREPARE_REPO_FORCE_MASTER_SCOPE",
            defaultValue: false,
            description: "Run integration prepare-repo stage even when BRANCH_NAME is not a master-equivalent branch"
        ),
        booleanParam(
            name: "PREPARE_REPO_DRY_RUN",
            defaultValue: false,
            description: "Do not commit/push deployment repo changes; show and archive diff only"
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
        ),
        string(
            name: "AWS_PLUGIN_IMAGE_OVERRIDE",
            defaultValue: "",
            description: "Optional full image override for [plugin:aws] in plugins.ini (for example, registry/repo:tag)"
        ),
        string(
            name: "GCP_PLUGIN_IMAGE_OVERRIDE",
            defaultValue: "",
            description: "Optional full image override for [plugin:gcp] in plugins.ini (for example, registry/repo:tag)"
        ),
        string(
            name: "AZURE_PLUGIN_IMAGE_OVERRIDE",
            defaultValue: "",
            description: "Optional full image override for [plugin:azure] in plugins.ini (for example, registry/repo:tag)"
        ),
        string(
            name: "KUBEVIRT_PLUGIN_IMAGE_OVERRIDE",
            defaultValue: "",
            description: "Optional full image override for [plugin:kubevirt] in plugins.ini (for example, registry/repo:tag)"
        ),
        string(
            name: "AMDS_PLUGIN_IMAGE_OVERRIDE",
            defaultValue: "",
            description: "Optional full image override for [plugin:amds] in plugins.ini (for example, registry/repo:tag)"
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
    def dockerRegistryExternal = env.DOCKER_REGISTRY_EXTERNAL ?: ""
    def dockerRegistryCredsExternal = env.DOCKER_REGISTRY_CREDENTIALS_EXTERNAL ?: "docker.io-docker-registry"
    def dockerPrefixExternal = env.DOCKER_PREFIX_EXTERNAL ?: "catalogicsoftware"

    def buildVelero = (params["${buildParamPrefix}VELERO"] ?: false) && isMasterFlow
    def buildCloudcasaVelero = (params["${buildParamPrefix}CLOUDCASA_VELERO"] ?: false) && isMasterFlow
    def runPrepareRepo = isMasterFlow || (params.PREPARE_REPO_FORCE_MASTER_SCOPE ? params.PREPARE_REPO_FORCE_MASTER_SCOPE.toBoolean() : false)
    def prepareRepoDryRun = params.PREPARE_REPO_DRY_RUN ? params.PREPARE_REPO_DRY_RUN.toBoolean() : false

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

    stage("Push velero image to external registry") {
        if (buildVelero) {
            env.BUILDX_CONFIG = "${env.HOME}/.docker/buildx"
            docker.withRegistry("https://${dockerRegistryInternal}", dockerRegistryCredsInternal) {
                docker.withRegistry(dockerRegistryExternal ? "https://${dockerRegistryExternal}" : "", dockerRegistryCredsExternal) {
                    sh """
                        set -eu
                        docker buildx imagetools create \
                            --tag ${dockerPrefixExternal}/velero:${veleroTag} \
                            ${dockerPrefixInternal}/velero:${veleroTag}
                    """
                }
            }
        }
    }

    stage("Build cloudcasa-velero image") {
        if (buildCloudcasaVelero) {
            env.BUILDX_CONFIG = "${env.HOME}/.docker/buildx"

            def pluginImageName = (params.VELEROPLUGIN_IMAGE_NAME ?: "amds-veleroplugin").trim()
            def pluginVersion = (params.VELEROPLUGIN_VERSION ?: "").trim()
            def pluginImageOverrides = [
                aws     : (params.AWS_PLUGIN_IMAGE_OVERRIDE ?: "").trim(),
                gcp     : (params.GCP_PLUGIN_IMAGE_OVERRIDE ?: "").trim(),
                azure   : (params.AZURE_PLUGIN_IMAGE_OVERRIDE ?: "").trim(),
                kubevirt: (params.KUBEVIRT_PLUGIN_IMAGE_OVERRIDE ?: "").trim(),
                amds    : (params.AMDS_PLUGIN_IMAGE_OVERRIDE ?: "").trim()
            ]

            def setPluginImageInConfig = { String pluginSection, String pluginImage ->
                withEnv([
                    "PLUGIN_SECTION=${pluginSection}",
                    "PLUGIN_IMAGE=${pluginImage}"
                ]) {
                    sh '''
                        set -eu
                        awk '
                            BEGIN {
                                in_plugin = 0
                                plugin_header = "[plugin:" ENVIRON["PLUGIN_SECTION"] "]"
                            }
                            $0 == plugin_header { in_plugin = 1; print; next }
                            substr($0, 1, 1) == "[" { in_plugin = 0 }
                            in_plugin && $0 ~ /^image[[:space:]]*=/ {
                                print "image = " ENVIRON["PLUGIN_IMAGE"]
                                next
                            }
                            { print }
                        ' plugins.ini > plugins.ini.tmp
                        mv plugins.ini.tmp plugins.ini
                    '''
                }
            }

            // Keep the config deterministic: use this build's velero image tag.
            def veleroBaseTagForCloudcasa = veleroTag
            withEnv([
                "VELERO_BASE_IMAGE=${dockerPrefixInternal}/velero:${veleroBaseTagForCloudcasa}"
            ]) {
                sh '''
                    set -eu
                    awk '
                        BEGIN { in_velero = 0 }
                        $0 == "[velero]" { in_velero = 1; print; next }
                        substr($0, 1, 1) == "[" { in_velero = 0 }
                        in_velero && $0 ~ /^image[[:space:]]*=/ {
                            print "image = " ENVIRON["VELERO_BASE_IMAGE"]
                            next
                        }
                        { print }
                    ' plugins.ini > plugins.ini.tmp
                    mv plugins.ini.tmp plugins.ini
                '''
            }

            if (pluginVersion && !pluginImageOverrides.amds) {
                withEnv([
                    "AMDS_PLUGIN_IMAGE=catalogicsoftware/${pluginImageName}:${pluginVersion}"
                ]) {
                    sh '''
                        set -eu
                        awk '
                            BEGIN { in_amds = 0 }
                            $0 == "[plugin:amds]" { in_amds = 1; print; next }
                            substr($0, 1, 1) == "[" { in_amds = 0 }
                            in_amds && $0 ~ /^image[[:space:]]*=/ {
                                print "image = " ENVIRON["AMDS_PLUGIN_IMAGE"]
                                next
                            }
                            { print }
                        ' plugins.ini > plugins.ini.tmp
                        mv plugins.ini.tmp plugins.ini
                    '''
                }
            }

            pluginImageOverrides.each { pluginSection, pluginImage ->
                if (pluginImage) {
                    setPluginImageInConfig(pluginSection as String, pluginImage as String)
                }
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

    stage("Push cloudcasa-velero image to external registry") {
        if (buildCloudcasaVelero) {
            env.BUILDX_CONFIG = "${env.HOME}/.docker/buildx"
            docker.withRegistry("https://${dockerRegistryInternal}", dockerRegistryCredsInternal) {
                docker.withRegistry(dockerRegistryExternal ? "https://${dockerRegistryExternal}" : "", dockerRegistryCredsExternal) {
                    sh """
                        set -eu
                        docker buildx imagetools create \
                            --tag ${dockerPrefixExternal}/cloudcasa-velero:${cloudcasaVeleroTag} \
                            ${dockerPrefixInternal}/cloudcasa-velero:${cloudcasaVeleroTag}
                    """
                }
            }
        }
    }

    stage("Push cloudcasa-velero to ACR") {
        if (buildCloudcasaVelero) {
            env.BUILDX_CONFIG = "${env.HOME}/.docker/buildx"
            def acrRegistry = env.CLOUDCASA_ACR_REGISTRY ?: "cloudcasaAgent.azurecr.io"
            def acrCredentialsId = env.CLOUDCASA_ACR_CREDENTIALS_ID ?: "cloudcasaAgent-azurecr"
            withCredentials([usernamePassword(
                credentialsId: acrCredentialsId,
                usernameVariable: 'ACR_USER',
                passwordVariable: 'ACR_PASS'
            )]) {
                docker.withRegistry("https://${dockerRegistryInternal}", dockerRegistryCredsInternal) {
                    sh """
                        set -eu
                        docker login -u \$ACR_USER -p \$ACR_PASS ${acrRegistry}
                        docker buildx imagetools create \
                            --tag ${acrRegistry}/catalogicsoftware/cloudcasa-velero:${cloudcasaVeleroTag} \
                            ${dockerPrefixInternal}/cloudcasa-velero:${cloudcasaVeleroTag}
                    """
                }
            }
        }
    }

    stage("Prepare deployment repo (integration)") {
        if (buildCloudcasaVelero && runPrepareRepo) {
            withCredentials([
                usernamePassword(
                    credentialsId: 'github-access-token',
                    usernameVariable: 'GIT_USER',
                    passwordVariable: 'GIT_PASS'
                )
            ]) {
                def deploymentRepoUrl = env.CLOUDCASA_DEPLOYMENT_REPO_URL
                def deploymentRepoHost = deploymentRepoUrl.replaceFirst('https://', '')
                dir('cloudcasa-deployment') {
                    git url: deploymentRepoUrl,
                        branch: 'master',
                        credentialsId: 'github-access-token'

                    sh """
                        git config user.name 'cloudcasabot'
                        git config user.email 'cloudcasabot@catalogicsoftware.com'
                    """

                    // Patch cloudcasa-velero image tag in all deployment files.
                    // Mirrors Concourse k8s-prepare-repo.sh: updates base/, local/, and integration/ files.
                    // Preserves the existing registry prefix (ACR or otherwise) by matching only on
                    // the catalogicsoftware/cloudcasa-velero: portion and replacing just the tag.
                    sh """
                        set -eu
                        TAG=${cloudcasaVeleroTag}

                        patch_cloudcasa_velero_tag() {
                            local file="\$1"
                            [ -f "\$file" ] || return 0
                            sed -Ei 's#(catalogicsoftware/cloudcasa-velero:)[^\"'"'"'[:space:]]+#\\1'"\${TAG}"'#g' "\$file"
                        }

                        patch_cloudcasa_velero_tag archimedes/base/kas/deployment.yaml
                        patch_cloudcasa_velero_tag archimedes/local/global-cm.yaml
                        patch_cloudcasa_velero_tag archimedes/integration/kas/deployment_kas_image_spec.yaml
                        patch_cloudcasa_velero_tag archimedes/integration/global-cm.yaml
                    """

                    sh """
                        set -eu
                        if [ -n "\$(git status --porcelain)" ]; then
                            if [ "${prepareRepoDryRun}" = "true" ]; then
                                echo "DRY RUN: deployment repo changes detected; skipping commit/push"
                                git status --short
                                git diff --patch > deployment-repo-dry-run.patch
                            else
                                git add -A
                                git commit -m "ci: update cloudcasa-velero to ${cloudcasaVeleroTag} from ${env.JOB_NAME} #${env.BUILD_NUMBER}"
                                git push https://${GIT_USER}:${GIT_PASS}@${deploymentRepoHost} master
                            fi
                        else
                            echo "No deployment repo changes for cloudcasa-velero"
                        fi
                    """

                    if (prepareRepoDryRun) {
                        archiveArtifacts artifacts: 'deployment-repo-dry-run.patch', allowEmptyArchive: true, onlyIfSuccessful: true
                    }
                }
            }
        }
    }
}
