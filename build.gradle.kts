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

}