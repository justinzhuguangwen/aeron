/*
 * Copyright 2014-2025 Real Logic Limited.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

/**
 * C port of Java ConsensusModuleConfigurationTest (authorisation tests).
 *
 * Tests verify that aeron_cm_authorisation_service_supplier() correctly
 * resolves DENY_ALL, ALLOW_ALL, and the default (allow-backup-and-standby)
 * authorisation services from the environment variable.
 */

#ifdef _MSC_VER
#ifndef _WINSOCKAPI_
#define _WINSOCKAPI_
#endif
#ifndef WIN32_LEAN_AND_MEAN
#define WIN32_LEAN_AND_MEAN
#endif
#include <windows.h>
#endif

#include <gtest/gtest.h>
#include <cstdlib>
#include <cstring>

#ifdef _MSC_VER
#include <process.h>
static int setenv(const char *name, const char *value, int overwrite)
{
    (void)overwrite;
    return _putenv_s(name, value);
}
static int unsetenv(const char *name)
{
    return _putenv_s(name, "");
}
#endif

extern "C"
{
#include "aeron_consensus_module_configuration.h"
}

/* -----------------------------------------------------------------------
 * shouldUseDenyAllAuthorisationSupplierWhenPropertySet
 *
 * Mirrors Java ConsensusModuleConfigurationTest
 *     .shouldUseDenyAllAuthorisationSupplierWhenPropertySet()
 *
 * When AERON_CLUSTER_AUTHORISATION_SERVICE_SUPPLIER=DENY_ALL is set the
 * resolved service should deny all requests.  After unsetting the
 * variable the default (allow backup & standby) should take effect:
 *   - BackupQuery (77)              -> allowed
 *   - SessionConnectRequest (3)     -> denied
 * ----------------------------------------------------------------------- */
TEST(ConsensusModuleConfigurationTest, shouldUseDenyAllAuthorisationSupplierWhenPropertySet)
{
    setenv(AERON_CM_AUTHORISATION_SERVICE_SUPPLIER_ENV_VAR, "DENY_ALL", 1);

    aeron_authorisation_service_func_t service = aeron_cm_authorisation_service_supplier();
    ASSERT_NE(nullptr, service);

    EXPECT_FALSE(service(
        AERON_CLUSTER_SBE_SCHEMA_ID,
        AERON_CLUSTER_BACKUP_QUERY_TEMPLATE_ID,
        nullptr, 0));
    EXPECT_FALSE(service(
        AERON_CLUSTER_SBE_SCHEMA_ID,
        AERON_CLUSTER_SESSION_CONNECT_REQUEST_TEMPLATE_ID,
        nullptr, 0));

    unsetenv(AERON_CM_AUTHORISATION_SERVICE_SUPPLIER_ENV_VAR);

    /* After clearing the env var, the default (allow backup & standby) applies */
    aeron_authorisation_service_func_t default_service = aeron_cm_authorisation_service_supplier();
    ASSERT_NE(nullptr, default_service);

    EXPECT_TRUE(default_service(
        AERON_CLUSTER_SBE_SCHEMA_ID,
        AERON_CLUSTER_BACKUP_QUERY_TEMPLATE_ID,
        nullptr, 0));
    EXPECT_FALSE(default_service(
        AERON_CLUSTER_SBE_SCHEMA_ID,
        AERON_CLUSTER_SESSION_CONNECT_REQUEST_TEMPLATE_ID,
        nullptr, 0));
}

/* -----------------------------------------------------------------------
 * shouldUseAllowAllAuthorisationSupplierWhenPropertySet
 *
 * Mirrors Java ConsensusModuleConfigurationTest
 *     .shouldUseAllowAllAuthorisationSupplierWhenPropertySet()
 * ----------------------------------------------------------------------- */
TEST(ConsensusModuleConfigurationTest, shouldUseAllowAllAuthorisationSupplierWhenPropertySet)
{
    setenv(AERON_CM_AUTHORISATION_SERVICE_SUPPLIER_ENV_VAR, "ALLOW_ALL", 1);

    aeron_authorisation_service_func_t service = aeron_cm_authorisation_service_supplier();
    ASSERT_NE(nullptr, service);

    EXPECT_TRUE(service(
        AERON_CLUSTER_SBE_SCHEMA_ID,
        AERON_CLUSTER_BACKUP_QUERY_TEMPLATE_ID,
        nullptr, 0));
    EXPECT_TRUE(service(
        AERON_CLUSTER_SBE_SCHEMA_ID,
        AERON_CLUSTER_SESSION_CONNECT_REQUEST_TEMPLATE_ID,
        nullptr, 0));

    unsetenv(AERON_CM_AUTHORISATION_SERVICE_SUPPLIER_ENV_VAR);

    /* After clearing the env var, the default (allow backup & standby) applies */
    aeron_authorisation_service_func_t default_service = aeron_cm_authorisation_service_supplier();
    ASSERT_NE(nullptr, default_service);

    EXPECT_TRUE(default_service(
        AERON_CLUSTER_SBE_SCHEMA_ID,
        AERON_CLUSTER_BACKUP_QUERY_TEMPLATE_ID,
        nullptr, 0));
    EXPECT_FALSE(default_service(
        AERON_CLUSTER_SBE_SCHEMA_ID,
        AERON_CLUSTER_SESSION_CONNECT_REQUEST_TEMPLATE_ID,
        nullptr, 0));
}
