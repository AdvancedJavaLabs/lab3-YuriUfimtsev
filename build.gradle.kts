plugins {
    id("java")
}

group = "com.yu_ufimtsev.itmo.sales_data_analysis"
version = "1.0-SNAPSHOT"

java {
    toolchain {
        languageVersion.set(JavaLanguageVersion.of(8))
    }
}

repositories {
    mavenCentral()
}

dependencies {
    implementation("org.apache.hadoop:hadoop-common:3.2.1")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-core:3.2.1")
    implementation("org.apache.hadoop:hadoop-mapreduce-client-jobclient:3.2.1")
}

tasks.jar {
    archiveBaseName.set("sales-analyzer-job")
    archiveVersion.set("")
    archiveClassifier.set("")

    manifest {
        attributes(
            "Main-Class" to "com.yu_ufimtsev.itmo.sales_data_analysis.SalesPipelineJob"
        )
    }
}
