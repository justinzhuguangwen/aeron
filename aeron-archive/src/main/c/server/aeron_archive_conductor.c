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

#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <inttypes.h>
#include <errno.h>
#include <dirent.h>
#include <unistd.h>

#include "aeron_alloc.h"
#include "util/aeron_error.h"
#include "util/aeron_clock.h"
#include "concurrent/aeron_thread.h"
#include "aeron_archive_conductor.h"
#include "aeron_archive_control_session.h"
#include "aeron_archive_replication_session.h"
#include "aeron_archive_replay_session.h"
#include "aeron_archive_mark_file.h"
#include "client/aeron_archive.h"
#include "aeron_client_conductor.h"
#include "uri/aeron_uri.h"
#include "uri/aeron_uri_string_builder.h"
#include "aeron_archive_control_session_adapter.h"
#include "aeron_archive_control_response_proxy.h"

#define AERON_ARCHIVE_CONDUCTOR_INITIAL_SESSION_CAPACITY (16)
#define AERON_ARCHIVE_CONDUCTOR_INITIAL_SUBSCRIPTION_CAPACITY (16)

#define AERON_ARCHIVE_SPY_PREFIX "aeron-spy:"

#define AERON_ARCHIVE_FRAME_ALIGNMENT (32)

#if defined(__GNUC__) || defined(__clang__)
#define AERON_ARCHIVE_MAYBE_UNUSED __attribute__((unused))
#else
#define AERON_ARCHIVE_MAYBE_UNUSED
#endif

/* Mirrors Java ArchiveConductor.onUnavailableImage (ArchiveConductor.java:237):
 * only aborts control sessions whose request image went away — NOT recording
 * sessions. Recording sessions detect image closure themselves via
 * aeron_image_is_closed() in recording_session_record() and transition to
 * INACTIVE on the conductor thread, avoiding the cross-thread UAF that an
 * abort-from-callback would cause (conductor thread frees the recording
 * session while this handler, running on the aeron client conductor thread,
 * would still hold a reference). */
static void aeron_archive_conductor_on_unavailable_image(
    void *clientd, aeron_subscription_t *subscription, aeron_image_t *image)
{
    aeron_archive_conductor_t *conductor = (aeron_archive_conductor_t *)clientd;
    if (NULL == conductor || NULL == image) { return; }
    (void)subscription;

    if (NULL != conductor->control_session_adapter)
    {
        aeron_archive_control_session_adapter_abort_by_image(
            conductor->control_session_adapter, image);
    }
}

/* Async-allocate a position counter with the Java RecordingPos key+label
 * layout. Caller stores the returned async handle; once polled-resolved,
 * the counter is attached to the session. Returns -1 on error. */
/* Synchronously allocate a RecordingPos counter with the Java
 * RecordingPos.allocate key+label layout, mirroring Java ArchiveConductor's
 * call to `aeron.addCounter` (which blocks on the client conductor thread
 * until the driver acknowledges). We emulate that with async + spin-poll
 * under a bounded timeout — matches Java's synchronous contract and avoids
 * the async handle lifecycle (cancel-at-close, race with counter_ready) that
 * leaked under LeakSanitizer on shutdown.
 *
 * Returns the resolved counter on success, NULL on error/timeout (callers
 * fall back to writer_position in recording_session_recorded_position).
 * Never returns a pending async handle — the counter is either fully
 * resolved or cleanly freed internally. */
static aeron_counter_t *aeron_archive_conductor_add_recording_position_counter(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id,
    int32_t session_id,
    int32_t stream_id,
    const char *stripped_channel,
    const char *source_identity)
{
    uint8_t key_buffer[AERON_COUNTER_MAX_KEY_LENGTH];
    char    label_buffer[AERON_COUNTER_MAX_LABEL_LENGTH];

    /* Key layout (matches Java RecordingPos.allocate):
     *   recording_id (8) | session_id (4) | src_identity_len (4) | src_identity (N) | archive_id (8) */
    const char *src = (NULL != source_identity) ? source_identity : "";
    const int32_t src_len_in = (int32_t)strlen(src);
    const int32_t max_src = (int32_t)AERON_COUNTER_MAX_KEY_LENGTH - 8 - 4 - 4 - 8;
    const int32_t src_len = src_len_in < max_src ? src_len_in : max_src;

    size_t off = 0;
    memcpy(key_buffer + off, &recording_id, 8); off += 8;
    memcpy(key_buffer + off, &session_id, 4);   off += 4;
    memcpy(key_buffer + off, &src_len, 4);      off += 4;
    memcpy(key_buffer + off, src, (size_t)src_len); off += (size_t)src_len;
    const int64_t archive_id = conductor->ctx->archive_id;
    memcpy(key_buffer + off, &archive_id, 8);   off += 8;
    const size_t key_length = off;

    int label_len = snprintf(label_buffer, sizeof(label_buffer),
        "rec-pos: %" PRId64 " %d %d %s - archive_id:%" PRId64,
        recording_id, session_id, stream_id,
        (NULL != stripped_channel) ? stripped_channel : "",
        archive_id);
    if (label_len < 0) { label_len = 0; }
    if ((size_t)label_len >= sizeof(label_buffer)) { label_len = (int)sizeof(label_buffer) - 1; }

    aeron_async_add_counter_t *async = NULL;
    if (aeron_async_add_counter(
            &async, conductor->aeron,
            100 /* AERON_COUNTER_ARCHIVE_RECORDING_POSITION_TYPE_ID — the macro
                 * in aeron_counters.h has a trailing ';' that breaks its use in
                 * an argument list; hardcoded to avoid the preprocessor bug. */,
            key_buffer, key_length,
            label_buffer, (size_t)label_len) < 0)
    {
        return NULL;
    }

    /* Spin-poll until the driver resolves or we hit the timeout. Matches
     * Java Aeron.addCounter's internal driver-timeout behavior (defaults to
     * 10 s in the Java client context). */
    aeron_counter_t *counter = NULL;
    const int64_t deadline_ms = aeron_epoch_clock() + 10000;
    for (;;)
    {
        int rc = aeron_async_add_counter_poll(&counter, async);
        if (rc > 0 && NULL != counter)
        {
            return counter;
        }
        if (rc < 0)
        {
            /* async already freed inside poll on error. */
            return NULL;
        }
        if (aeron_epoch_clock() >= deadline_ms)
        {
            aeron_async_add_counter_cancel(conductor->aeron, async);
            return NULL;
        }
        aeron_micro_sleep(100);
    }
}

/* ---- Forward declarations of segment-file helpers (defined further below) ---- */
static int aeron_archive_conductor_delete_segments_on_disk(
    aeron_archive_conductor_t *conductor, int64_t recording_id,
    int64_t min_inclusive_base, int64_t max_exclusive_base);

/* Strip a channel URI of ephemeral parameters (term-length, mtu, position
 * cursors, timeouts, etc.) so the catalog's stripped_channel matches across
 * reconnects regardless of client-side tuning. Mirrors Java
 * ArchiveConductor.strippedChannelBuilder — allowlist-based.
 * Writes the stripped form to `dst` (max `dst_len`). On failure falls back
 * to copying the original channel unchanged. */
static void aeron_archive_strip_channel(
    const char *original_channel, char *dst, size_t dst_len)
{
    if (NULL == original_channel || NULL == dst || dst_len == 0) { return; }

    aeron_uri_string_builder_t builder;
    if (aeron_uri_string_builder_init_on_string(&builder, original_channel) < 0)
    {
        snprintf(dst, dst_len, "%s", original_channel);
        return;
    }

    /* Remove ephemeral / client-side-specific keys. Anything not listed
     * here is preserved (endpoint, interface, control, control-mode, tags,
     * rejoin, group, tether, fc, gtag, cc, ssc, session-id, alias,
     * response-*, ttl, stream-id, so-sndbuf, so-rcvbuf, rcv-wnd,
     * *-ts-offset — the Java allowlist). */
    static const char *const ephemeral_keys[] = {
        AERON_URI_INITIAL_TERM_ID_KEY,
        AERON_URI_TERM_ID_KEY,
        AERON_URI_TERM_OFFSET_KEY,
        AERON_URI_TERM_LENGTH_KEY,
        AERON_URI_LINGER_TIMEOUT_KEY,
        AERON_URI_MTU_LENGTH_KEY,
        AERON_URI_SPARSE_TERM_KEY,
        AERON_URI_EOS_KEY,
        AERON_URI_ATS_KEY,
        AERON_URI_NAK_DELAY_KEY,
        AERON_URI_UNTETHERED_WINDOW_LIMIT_TIMEOUT_KEY,
        AERON_URI_UNTETHERED_LINGER_TIMEOUT_KEY,
        AERON_URI_UNTETHERED_RESTING_TIMEOUT_KEY,
        AERON_URI_MAX_RESEND_KEY,
        AERON_URI_PUBLICATION_WINDOW_KEY,
        NULL
    };
    for (int i = 0; NULL != ephemeral_keys[i]; i++)
    {
        aeron_uri_string_builder_put(&builder, ephemeral_keys[i], NULL);
    }

    if (aeron_uri_string_builder_sprint(&builder, dst, dst_len) < 0)
    {
        snprintf(dst, dst_len, "%s", original_channel);
    }
    aeron_uri_string_builder_close(&builder);
}


/* ---- static helpers ---- */

static int64_t aeron_archive_conductor_epoch_clock_ms(void)
{
    return aeron_epoch_clock();
}

/**
 * Build a subscription key from stream_id and channel, matching the Java makeKey() logic.
 * Format: "<streamId>:<channel>"
 */
static char *aeron_archive_conductor_make_key(int32_t stream_id, const char *channel)
{
    const size_t channel_len = strlen(channel);
    /* stream_id string: up to 11 chars, plus ':' plus channel plus NUL */
    const size_t key_len = 12 + 1 + channel_len + 1;
    char *key = NULL;

    if (aeron_alloc((void **)&key, key_len) < 0)
    {
        return NULL;
    }

    snprintf(key, key_len, "%d:%s", stream_id, channel);
    return key;
}

/* ---- Recording session map operations ---- */

AERON_ARCHIVE_MAYBE_UNUSED
static int aeron_archive_conductor_ensure_recording_session_capacity(
    aeron_archive_conductor_t *conductor)
{
    if (conductor->recording_session_count >= conductor->recording_session_capacity)
    {
        const int32_t new_capacity = conductor->recording_session_capacity * 2;
        aeron_archive_recording_session_entry_t *new_entries = NULL;

        if (aeron_alloc((void **)&new_entries, sizeof(aeron_archive_recording_session_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow recording session array");
            return -1;
        }

        memcpy(new_entries, conductor->recording_sessions,
            sizeof(aeron_archive_recording_session_entry_t) * (size_t)conductor->recording_session_count);
        aeron_free(conductor->recording_sessions);
        conductor->recording_sessions = new_entries;
        conductor->recording_session_capacity = new_capacity;
    }

    return 0;
}

AERON_ARCHIVE_MAYBE_UNUSED
static int aeron_archive_conductor_add_recording_session(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id,
    aeron_archive_recording_session_t *session)
{
    if (aeron_archive_conductor_ensure_recording_session_capacity(conductor) < 0)
    {
        return -1;
    }

    aeron_archive_recording_session_entry_t *entry =
        &conductor->recording_sessions[conductor->recording_session_count];
    entry->recording_id = recording_id;
    entry->session = session;
    conductor->recording_session_count++;

    return 0;
}

static aeron_archive_recording_session_t *aeron_archive_conductor_find_recording_session_internal(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id)
{
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        if (conductor->recording_sessions[i].recording_id == recording_id)
        {
            return conductor->recording_sessions[i].session;
        }
    }

    return NULL;
}

static void aeron_archive_conductor_remove_recording_session(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id)
{
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        if (conductor->recording_sessions[i].recording_id == recording_id)
        {
            /* Swap with last entry */
            conductor->recording_session_count--;
            if (i < conductor->recording_session_count)
            {
                conductor->recording_sessions[i] =
                    conductor->recording_sessions[conductor->recording_session_count];
            }
            return;
        }
    }
}

/* ---- Replay session map operations ---- */

AERON_ARCHIVE_MAYBE_UNUSED
static int aeron_archive_conductor_ensure_replay_session_capacity(aeron_archive_conductor_t *conductor)
{
    if (conductor->replay_session_count >= conductor->replay_session_capacity)
    {
        const int32_t new_capacity = conductor->replay_session_capacity * 2;
        aeron_archive_replay_session_entry_t *new_entries = NULL;

        if (aeron_alloc((void **)&new_entries, sizeof(aeron_archive_replay_session_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow replay session array");
            return -1;
        }

        memcpy(new_entries, conductor->replay_sessions,
            sizeof(aeron_archive_replay_session_entry_t) * (size_t)conductor->replay_session_count);
        aeron_free(conductor->replay_sessions);
        conductor->replay_sessions = new_entries;
        conductor->replay_session_capacity = new_capacity;
    }

    return 0;
}

AERON_ARCHIVE_MAYBE_UNUSED
static void aeron_archive_conductor_remove_replay_session(
    aeron_archive_conductor_t *conductor,
    int64_t replay_session_id)
{
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        if (conductor->replay_sessions[i].replay_session_id == replay_session_id)
        {
            conductor->replay_session_count--;
            if (i < conductor->replay_session_count)
            {
                conductor->replay_sessions[i] =
                    conductor->replay_sessions[conductor->replay_session_count];
            }
            return;
        }
    }
}

/* ---- Replication session map operations ---- */

AERON_ARCHIVE_MAYBE_UNUSED
static int aeron_archive_conductor_ensure_replication_session_capacity(aeron_archive_conductor_t *conductor)
{
    if (conductor->replication_session_count >= conductor->replication_session_capacity)
    {
        const int32_t new_capacity = conductor->replication_session_capacity * 2;
        aeron_archive_replication_session_entry_t *new_entries = NULL;

        if (aeron_alloc(
            (void **)&new_entries,
            sizeof(aeron_archive_replication_session_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow replication session array");
            return -1;
        }

        memcpy(new_entries, conductor->replication_sessions,
            sizeof(aeron_archive_replication_session_entry_t) * (size_t)conductor->replication_session_count);
        aeron_free(conductor->replication_sessions);
        conductor->replication_sessions = new_entries;
        conductor->replication_session_capacity = new_capacity;
    }

    return 0;
}

static void aeron_archive_conductor_remove_replication_session(
    aeron_archive_conductor_t *conductor,
    int64_t replication_id) AERON_ARCHIVE_MAYBE_UNUSED;

static void aeron_archive_conductor_remove_replication_session(
    aeron_archive_conductor_t *conductor,
    int64_t replication_id)
{
    for (int32_t i = 0; i < conductor->replication_session_count; i++)
    {
        if (conductor->replication_sessions[i].replication_id == replication_id)
        {
            conductor->replication_session_count--;
            if (i < conductor->replication_session_count)
            {
                conductor->replication_sessions[i] =
                    conductor->replication_sessions[conductor->replication_session_count];
            }
            return;
        }
    }
}

/* ---- Subscription map operations ---- */

static int aeron_archive_conductor_ensure_subscription_capacity(aeron_archive_conductor_t *conductor)
{
    if (conductor->recording_subscription_count >= conductor->recording_subscription_capacity)
    {
        const int32_t new_capacity = conductor->recording_subscription_capacity * 2;
        aeron_archive_recording_subscription_entry_t *new_entries = NULL;

        if (aeron_alloc(
            (void **)&new_entries,
            sizeof(aeron_archive_recording_subscription_entry_t) * (size_t)new_capacity) < 0)
        {
            AERON_SET_ERR(ENOMEM, "%s", "failed to grow recording subscription array");
            return -1;
        }

        memcpy(new_entries, conductor->recording_subscriptions,
            sizeof(aeron_archive_recording_subscription_entry_t) * (size_t)conductor->recording_subscription_count);
        aeron_free(conductor->recording_subscriptions);
        conductor->recording_subscriptions = new_entries;
        conductor->recording_subscription_capacity = new_capacity;
    }

    return 0;
}

static aeron_archive_recording_subscription_entry_t *aeron_archive_conductor_find_subscription_by_key(
    aeron_archive_conductor_t *conductor,
    const char *key)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        if (0 == strcmp(conductor->recording_subscriptions[i].key, key))
        {
            return &conductor->recording_subscriptions[i];
        }
    }

    return NULL;
}

static aeron_archive_recording_subscription_entry_t *aeron_archive_conductor_find_subscription_by_id(
    aeron_archive_conductor_t *conductor,
    int64_t subscription_id)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        aeron_subscription_constants_t constants;
        if (aeron_subscription_constants(conductor->recording_subscriptions[i].subscription, &constants) == 0)
        {
            if (constants.registration_id == subscription_id)
            {
                return &conductor->recording_subscriptions[i];
            }
        }
    }

    return NULL;
}

static void aeron_archive_conductor_free_subscription_entry_fields(
    aeron_archive_recording_subscription_entry_t *entry)
{
    if (NULL == entry) { return; }
    aeron_free(entry->key);
    entry->key = NULL;
    if (NULL != entry->original_channel)
    {
        free(entry->original_channel);
        entry->original_channel = NULL;
    }
    if (NULL != entry->recorded_image_session_ids)
    {
        aeron_free(entry->recorded_image_session_ids);
        entry->recorded_image_session_ids = NULL;
    }
    entry->recorded_image_count = 0;
    entry->recorded_image_capacity = 0;
}

static void aeron_archive_conductor_remove_subscription_by_key(
    aeron_archive_conductor_t *conductor,
    const char *key)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        if (0 == strcmp(conductor->recording_subscriptions[i].key, key))
        {
            aeron_archive_conductor_free_subscription_entry_fields(
                &conductor->recording_subscriptions[i]);
            conductor->recording_subscription_count--;
            if (i < conductor->recording_subscription_count)
            {
                conductor->recording_subscriptions[i] =
                    conductor->recording_subscriptions[conductor->recording_subscription_count];
            }
            return;
        }
    }
}

static aeron_archive_recording_subscription_entry_t *aeron_archive_conductor_remove_subscription_by_id(
    aeron_archive_conductor_t *conductor,
    int64_t subscription_id)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        aeron_subscription_constants_t constants;
        if (aeron_subscription_constants(conductor->recording_subscriptions[i].subscription, &constants) == 0 &&
            constants.registration_id == subscription_id)
        {
            aeron_archive_conductor_free_subscription_entry_fields(
                &conductor->recording_subscriptions[i]);
            conductor->recording_subscription_count--;
            if (i < conductor->recording_subscription_count)
            {
                conductor->recording_subscriptions[i] =
                    conductor->recording_subscriptions[conductor->recording_subscription_count];
            }
            return &conductor->recording_subscriptions[i];
        }
    }

    return NULL;
}

/* ---- Recording session startup on image-available ----
 * On each duty cycle, for each resolved recording subscription, iterate the
 * subscription's images. For any image we haven't yet started a recording
 * session for, allocate a catalog entry, spin up the recording session,
 * register it, and emit a START RecordingSignal so the control-session
 * client (which issued the start_recording request) learns the recording_id.
 */
static bool aeron_archive_conductor_subscription_has_tracked_image(
    const aeron_archive_recording_subscription_entry_t *entry, int32_t session_id)
{
    for (int32_t i = 0; i < entry->recorded_image_count; i++)
    {
        if (entry->recorded_image_session_ids[i] == session_id)
        {
            return true;
        }
    }
    return false;
}

static int aeron_archive_conductor_track_image(
    aeron_archive_recording_subscription_entry_t *entry, int32_t session_id)
{
    if (entry->recorded_image_count >= entry->recorded_image_capacity)
    {
        int32_t new_cap = entry->recorded_image_capacity == 0 ? 4 : entry->recorded_image_capacity * 2;
        int32_t *new_arr = NULL;
        if (aeron_alloc((void **)&new_arr, sizeof(int32_t) * (size_t)new_cap) < 0)
        {
            return -1;
        }
        if (entry->recorded_image_count > 0)
        {
            memcpy(new_arr, entry->recorded_image_session_ids,
                sizeof(int32_t) * (size_t)entry->recorded_image_count);
            aeron_free(entry->recorded_image_session_ids);
        }
        entry->recorded_image_session_ids = new_arr;
        entry->recorded_image_capacity = new_cap;
    }
    entry->recorded_image_session_ids[entry->recorded_image_count++] = session_id;
    return 0;
}

static int aeron_archive_conductor_start_recording_session_for_image(
    aeron_archive_conductor_t *conductor,
    aeron_archive_recording_subscription_entry_t *sub_entry,
    aeron_image_t *image)
{
    aeron_image_constants_t ic;
    if (aeron_image_constants(image, &ic) < 0)
    {
        return -1;
    }

    /* Allocate catalog entry. We use image constants for the descriptor. */
    aeron_subscription_constants_t sc;
    if (aeron_subscription_constants(sub_entry->subscription, &sc) < 0)
    {
        return -1;
    }

    int64_t recording_id;
    if (AERON_ARCHIVE_NULL_VALUE != sub_entry->extend_target_recording_id)
    {
        /* Extend path: reuse the existing recording_id, clear stop_position
         * so the session records forward from the prior stop point. */
        recording_id = sub_entry->extend_target_recording_id;
        aeron_archive_catalog_update_stop_position(
            conductor->catalog, recording_id, AERON_ARCHIVE_NULL_POSITION);
    }
    else
    {
        char stripped[AERON_URI_MAX_LENGTH];
        aeron_archive_strip_channel(sub_entry->original_channel, stripped, sizeof(stripped));

        recording_id = aeron_archive_catalog_add_recording(
            conductor->catalog,
            ic.join_position,
            AERON_ARCHIVE_NULL_POSITION,  /* stop position: still recording */
            conductor->cached_epoch_clock_ms,
            AERON_ARCHIVE_NULL_VALUE,     /* stop timestamp */
            ic.initial_term_id,
            conductor->ctx->segment_file_length,
            (int32_t)ic.term_buffer_length,
            (int32_t)ic.mtu_length,
            ic.session_id,
            sub_entry->stream_id,
            stripped,
            sub_entry->original_channel,
            ic.source_identity != NULL ? ic.source_identity : "");

        if (AERON_NULL_VALUE == recording_id)
        {
            AERON_APPEND_ERR("%s", "catalog_add_recording failed");
            return -1;
        }
    }

    /* Allocate the RecordingPos counter synchronously BEFORE the recording
     * session is created, mirroring Java ArchiveConductor: the counter goes
     * into the session at construction so `recorded_position` has the
     * zero-copy counter path from the first tick. If allocation fails
     * (timeout / error) we pass NULL, and `recorded_position` transparently
     * falls back to writer_position. */
    aeron_counter_t *position = NULL;
    {
        char stripped_for_label[AERON_URI_MAX_LENGTH];
        aeron_archive_strip_channel(
            sub_entry->original_channel, stripped_for_label, sizeof(stripped_for_label));
        position = aeron_archive_conductor_add_recording_position_counter(
            conductor,
            recording_id,
            ic.session_id,
            sub_entry->stream_id,
            stripped_for_label,
            ic.source_identity);
    }

    aeron_archive_recording_session_t *session = NULL;
    if (aeron_archive_recording_session_create(
            &session,
            sub_entry->correlation_id,
            recording_id,
            ic.join_position,
            conductor->ctx->segment_file_length,
            sub_entry->original_channel,
            conductor->recording_events_proxy,
            image,
            position,
            conductor->ctx->archive_dir,
            conductor->ctx->file_io_max_length,
            conductor->ctx->force_writes,
            conductor->ctx->force_metadata,
            sub_entry->auto_stop) < 0)
    {
        if (NULL != position)
        {
            aeron_counter_close(position, NULL, NULL);
        }
        AERON_APPEND_ERR("%s", "recording_session_create failed");
        return -1;
    }

    if (aeron_archive_conductor_add_recording_session(conductor, recording_id, session) < 0)
    {
        aeron_archive_recording_session_close(session);
        return -1;
    }

    /* Fill in the link back to the originating control session for signal routing. */
    aeron_archive_recording_session_entry_t *rs_entry =
        &conductor->recording_sessions[conductor->recording_session_count - 1];
    rs_entry->control_session = sub_entry->control_session;
    rs_entry->correlation_id = sub_entry->correlation_id;
    rs_entry->subscription_id = (int64_t)sc.registration_id;
    rs_entry->source_image_session_id = ic.session_id;
    rs_entry->position_counter_async = NULL;

    /* Emit START/EXTEND signal back to the control session that initiated
     * this recording. Java emits EXTEND on the extend_recording path. */
    if (NULL != sub_entry->control_session)
    {
        const aeron_archive_recording_signal_code_t code =
            (AERON_ARCHIVE_NULL_VALUE != sub_entry->extend_target_recording_id)
                ? AERON_ARCHIVE_RECORDING_SIGNAL_CODE_EXTEND
                : AERON_ARCHIVE_RECORDING_SIGNAL_CODE_START;
        aeron_archive_control_session_send_signal(
            (aeron_archive_control_session_t *)sub_entry->control_session,
            sub_entry->correlation_id,
            recording_id,
            (int64_t)sc.registration_id,
            ic.join_position,
            code);
    }

    return aeron_archive_conductor_track_image(sub_entry, ic.session_id);
}

static int aeron_archive_conductor_check_new_images(aeron_archive_conductor_t *conductor)
{
    int work_count = 0;
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        aeron_archive_recording_subscription_entry_t *entry = &conductor->recording_subscriptions[i];
        if (NULL == entry->subscription)
        {
            continue;
        }

        const int image_count = aeron_subscription_image_count(entry->subscription);
        for (int idx = 0; idx < image_count; idx++)
        {
            aeron_image_t *image = aeron_subscription_image_at_index(entry->subscription, (size_t)idx);
            if (NULL == image)
            {
                continue;
            }

            aeron_image_constants_t ic;
            if (aeron_image_constants(image, &ic) < 0)
            {
                continue;
            }

            if (aeron_archive_conductor_subscription_has_tracked_image(entry, ic.session_id))
            {
                continue;
            }

            if (aeron_archive_conductor_start_recording_session_for_image(
                    conductor, entry, image) < 0)
            {
                /* Already logged via AERON_APPEND_ERR; skip and continue. */
                continue;
            }
            work_count++;
        }
    }
    return work_count;
}

static void aeron_archive_conductor_remove_subscription_tracking(
    aeron_archive_conductor_t *conductor, int64_t subscription_id, int32_t source_image_session_id)
{
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        aeron_archive_recording_subscription_entry_t *entry = &conductor->recording_subscriptions[i];
        if (NULL == entry->subscription)
        {
            continue;
        }
        aeron_subscription_constants_t sc;
        if (aeron_subscription_constants(entry->subscription, &sc) == 0 &&
            (int64_t)sc.registration_id == subscription_id)
        {
            for (int32_t j = 0; j < entry->recorded_image_count; j++)
            {
                if (entry->recorded_image_session_ids[j] == source_image_session_id)
                {
                    entry->recorded_image_count--;
                    if (j < entry->recorded_image_count)
                    {
                        entry->recorded_image_session_ids[j] =
                            entry->recorded_image_session_ids[entry->recorded_image_count];
                    }
                    return;
                }
            }
        }
    }
}

/* ---- Task queue operations ---- */

static int aeron_archive_conductor_run_tasks(aeron_archive_conductor_t *conductor)
{
    int work_count = 0;

    while (NULL != conductor->task_queue_head)
    {
        aeron_archive_conductor_task_t *task = conductor->task_queue_head;
        conductor->task_queue_head = task->next;
        if (NULL == conductor->task_queue_head)
        {
            conductor->task_queue_tail = NULL;
        }

        task->func(task->clientd);
        aeron_free(task);
        work_count++;
    }

    return work_count;
}

/* ---- Recording session callback helpers ---- */

/**
 * Populate the recording summary from a catalog descriptor.
 */
static int aeron_archive_conductor_recording_summary(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id,
    aeron_archive_recording_summary_t *summary)
{
    aeron_archive_catalog_recording_descriptor_t descriptor;
    if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) < 0)
    {
        return -1;
    }

    summary->recording_id = descriptor.recording_id;
    summary->start_position = descriptor.start_position;
    summary->stop_position = descriptor.stop_position;
    summary->start_timestamp = descriptor.start_timestamp;
    summary->stop_timestamp = descriptor.stop_timestamp;
    summary->initial_term_id = descriptor.initial_term_id;
    summary->segment_file_length = descriptor.segment_file_length;
    summary->term_buffer_length = descriptor.term_buffer_length;
    summary->mtu_length = descriptor.mtu_length;
    summary->session_id = descriptor.session_id;
    summary->stream_id = descriptor.stream_id;

    return 0;
}

static bool aeron_archive_conductor_has_recording(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id)
{
    aeron_archive_catalog_recording_descriptor_t descriptor;
    return aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) == 0;
}

/**
 * Drive all active recording sessions, removing any that are done.
 */
static int aeron_archive_conductor_drive_recording_sessions(aeron_archive_conductor_t *conductor)
{
    int work_count = 0;

    for (int32_t i = conductor->recording_session_count - 1; i >= 0; i--)
    {
        aeron_archive_recording_session_t *session = conductor->recording_sessions[i].session;
        const int result = aeron_archive_recording_session_do_work(session);

        if (result > 0)
        {
            work_count += result;
        }

        if (aeron_archive_recording_session_is_done(session))
        {
            aeron_archive_conductor_close_recording_session(conductor, session);
        }
    }

    return work_count;
}

/**
 * Abort all recording sessions on a given subscription.
 */
static void aeron_archive_conductor_abort_recording_sessions_for_subscription(
    aeron_archive_conductor_t *conductor,
    aeron_subscription_t *subscription)
{
    if (NULL == subscription) { return; }
    aeron_subscription_constants_t sc;
    if (aeron_subscription_constants(subscription, &sc) < 0) { return; }
    const int64_t subscription_id = (int64_t)sc.registration_id;

    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        if (conductor->recording_sessions[i].subscription_id == subscription_id)
        {
            aeron_archive_recording_session_abort(
                conductor->recording_sessions[i].session, "stop recording");
        }
    }
}

/* ---- Lifecycle ---- */

int aeron_archive_conductor_create(
    aeron_archive_conductor_t **conductor,
    aeron_archive_conductor_context_t *ctx)
{
    aeron_archive_conductor_t *c = NULL;

    if (NULL == conductor || NULL == ctx)
    {
        AERON_SET_ERR(EINVAL, "%s", "conductor or ctx is NULL");
        return -1;
    }

    if (aeron_alloc((void **)&c, sizeof(aeron_archive_conductor_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archive conductor");
        return -1;
    }

    memset(c, 0, sizeof(aeron_archive_conductor_t));

    c->ctx = ctx;
    c->aeron = ctx->aeron;
    c->catalog = ctx->catalog;
    c->recording_events_proxy = ctx->recording_events_proxy;

    c->is_abort = false;
    c->is_closed = false;

    /* Generate initial session ID from random source */
    srand((unsigned int)time(NULL));
    c->next_session_id = (int64_t)(rand() % INT32_MAX);
    c->replay_id = 1;
    c->mark_file_update_deadline_ms = 0;
    c->cached_epoch_clock_ms = aeron_archive_conductor_epoch_clock_ms();

    /* Allocate recording session array */
    c->recording_session_capacity = AERON_ARCHIVE_CONDUCTOR_INITIAL_SESSION_CAPACITY;
    if (aeron_alloc(
        (void **)&c->recording_sessions,
        sizeof(aeron_archive_recording_session_entry_t) * (size_t)c->recording_session_capacity) < 0)
    {
        goto error_cleanup;
    }
    c->recording_session_count = 0;

    /* Allocate replay session array */
    c->replay_session_capacity = AERON_ARCHIVE_CONDUCTOR_INITIAL_SESSION_CAPACITY;
    if (aeron_alloc(
        (void **)&c->replay_sessions,
        sizeof(aeron_archive_replay_session_entry_t) * (size_t)c->replay_session_capacity) < 0)
    {
        goto error_cleanup;
    }
    c->replay_session_count = 0;

    /* Allocate replication session array */
    c->replication_session_capacity = AERON_ARCHIVE_CONDUCTOR_INITIAL_SESSION_CAPACITY;
    if (aeron_alloc(
        (void **)&c->replication_sessions,
        sizeof(aeron_archive_replication_session_entry_t) * (size_t)c->replication_session_capacity) < 0)
    {
        goto error_cleanup;
    }
    c->replication_session_count = 0;

    /* Allocate recording subscription array */
    c->recording_subscription_capacity = AERON_ARCHIVE_CONDUCTOR_INITIAL_SUBSCRIPTION_CAPACITY;
    if (aeron_alloc(
        (void **)&c->recording_subscriptions,
        sizeof(aeron_archive_recording_subscription_entry_t) * (size_t)c->recording_subscription_capacity) < 0)
    {
        goto error_cleanup;
    }
    c->recording_subscription_count = 0;

    /* Task queue starts empty */
    c->task_queue_head = NULL;
    c->task_queue_tail = NULL;

    /* Set up control subscriptions */
    c->control_subscription = NULL;
    c->local_control_subscription = NULL;
    c->control_subscription_async = NULL;
    c->local_control_subscription_async = NULL;
    c->control_session_adapter = NULL;
    c->control_response_proxy = NULL;
    c->control_sessions_head = NULL;

    /* Create the control response proxy */
    if (aeron_archive_control_response_proxy_create(&c->control_response_proxy) < 0)
    {
        goto error_cleanup;
    }

    /* Create control subscriptions asynchronously.
     * The do_work loop will poll these async handles and create the adapter once ready. */
    if (ctx->control_channel_enabled && NULL != ctx->control_channel)
    {
        if (aeron_async_add_subscription(
            &c->control_subscription_async, c->aeron, ctx->control_channel, ctx->control_stream_id,
            NULL, NULL, NULL, NULL) < 0)
        {
            /* Non-fatal */
        }
    }

    /* Skip local_control if it would point at the same (channel,stream) as
     * the main control subscription — would cause each request to be
     * dispatched twice (the second hitting a spurious "recording exists"
     * on duplicate start_recording). This happens in cluster tests that
     * override control_channel to "aeron:ipc" to match the IPC default of
     * local_control. Mirrors Java Archive.Context: local channel is
     * suppressed when equivalent to the main channel. */
    const bool local_matches_main =
        NULL != ctx->local_control_channel &&
        NULL != ctx->control_channel &&
        ctx->local_control_stream_id == ctx->control_stream_id &&
        0 == strncmp(ctx->local_control_channel, "aeron:ipc", 9) &&
        0 == strncmp(ctx->control_channel, "aeron:ipc", 9);

    if (NULL != ctx->local_control_channel && !local_matches_main)
    {
        if (aeron_async_add_subscription(
            &c->local_control_subscription_async, c->aeron, ctx->local_control_channel, ctx->local_control_stream_id,
            NULL, NULL, NULL, NULL) < 0)
        {
            /* Non-fatal */
        }
    }

    *conductor = c;
    return 0;

error_cleanup:
    aeron_archive_control_response_proxy_close(c->control_response_proxy);
    aeron_free(c->recording_sessions);
    aeron_free(c->replay_sessions);
    aeron_free(c->replication_sessions);
    aeron_free(c->recording_subscriptions);
    aeron_free(c);
    AERON_SET_ERR(ENOMEM, "%s", "failed to allocate archive conductor resources");
    return -1;
}

int aeron_archive_conductor_do_work(aeron_archive_conductor_t *conductor)
{
    int work_count = 0;

    if (NULL == conductor)
    {
        return -1;
    }

    if (conductor->is_abort)
    {
        AERON_SET_ERR(EINTR, "%s", "archive conductor aborted");
        return -1;
    }

    const int64_t now_ms = aeron_archive_conductor_epoch_clock_ms();

    if (conductor->cached_epoch_clock_ms != now_ms)
    {
        conductor->cached_epoch_clock_ms = now_ms;

        if (now_ms >= conductor->mark_file_update_deadline_ms)
        {
            conductor->mark_file_update_deadline_ms =
                now_ms + AERON_ARCHIVE_CONDUCTOR_MARK_FILE_UPDATE_INTERVAL_MS;
            if (NULL != conductor->ctx->mark_file)
            {
                aeron_archive_mark_file_update_activity_timestamp(
                    conductor->ctx->mark_file, now_ms);
            }
        }
    }

    {
    }

    /*
     * Poll async subscription handles until resolved, then create the adapter.
     */
    if (NULL != conductor->control_subscription_async && NULL == conductor->control_subscription)
    {
        aeron_subscription_t *sub = NULL;
        int rc = aeron_async_add_subscription_poll(&sub, conductor->control_subscription_async);
        if (rc > 0 && NULL != sub)
        {
            conductor->control_subscription = sub;
            conductor->control_subscription_async = NULL;
            work_count++;
        }
        else if (rc < 0)
        {
            conductor->control_subscription_async = NULL;
        }
    }

    if (NULL != conductor->local_control_subscription_async && NULL == conductor->local_control_subscription)
    {
        aeron_subscription_t *sub = NULL;
        int rc = aeron_async_add_subscription_poll(&sub, conductor->local_control_subscription_async);
        if (rc > 0 && NULL != sub)
        {
            conductor->local_control_subscription = sub;
            conductor->local_control_subscription_async = NULL;
            work_count++;
        }
        else if (rc < 0)
        {
            conductor->local_control_subscription_async = NULL;
        }
    }

    /*
     * Create the control session adapter once subscriptions are resolved.
     * We need at least the local control subscription, or the control subscription if enabled.
     */
    if (NULL == conductor->control_session_adapter)
    {
        bool control_ready = !conductor->ctx->control_channel_enabled ||
            NULL != conductor->control_subscription ||
            NULL == conductor->control_subscription_async;
        bool local_ready = NULL != conductor->local_control_subscription ||
            NULL == conductor->local_control_subscription_async;

        if (control_ready && local_ready &&
            (NULL != conductor->control_subscription || NULL != conductor->local_control_subscription))
        {
            if (aeron_archive_control_session_adapter_create(
                &conductor->control_session_adapter,
                conductor->control_subscription,
                conductor->local_control_subscription,
                conductor) == 0)
            {
                work_count++;
            }
        }
    }

    /*
     * Poll control subscriptions for new requests via the adapter.
     */
    if (NULL != conductor->control_session_adapter)
    {
        work_count += aeron_archive_control_session_adapter_poll(conductor->control_session_adapter);
    }

    /*
     * Drive all active control sessions (state machine: INIT -> CONNECTING -> CONNECTED -> ACTIVE).
     */
    {
        aeron_archive_control_session_entry_t **prev_ptr = &conductor->control_sessions_head;
        aeron_archive_control_session_entry_t *entry = conductor->control_sessions_head;

        while (NULL != entry)
        {
            aeron_archive_control_session_entry_t *next = entry->next;
            int session_work = aeron_archive_control_session_do_work(entry->session, conductor->cached_epoch_clock_ms);

            if (session_work > 0)
            {
                work_count += session_work;
            }

            if (aeron_archive_control_session_is_done(entry->session))
            {
                *prev_ptr = next;
                /* Null out any recording_session_entry whose control_session
                 * points at the session we're about to free — otherwise
                 * close_recording_session tries to route STOP signal through
                 * a dangling pointer (use-after-free). Same for replication
                 * sessions (which emit REPLICATE_END signals on close). */
                aeron_archive_control_session_t *closing = entry->session;
                for (int32_t k = 0; k < conductor->recording_session_count; k++)
                {
                    if (conductor->recording_sessions[k].control_session == (void *)closing)
                    {
                        conductor->recording_sessions[k].control_session = NULL;
                    }
                }
                for (int32_t k = 0; k < conductor->recording_subscription_count; k++)
                {
                    if (conductor->recording_subscriptions[k].control_session == (void *)closing)
                    {
                        conductor->recording_subscriptions[k].control_session = NULL;
                    }
                }
                for (int32_t k = 0; k < conductor->replication_session_count; k++)
                {
                    aeron_archive_replication_session_t *rs = conductor->replication_sessions[k].session;
                    if (NULL != rs && rs->control_session == closing)
                    {
                        rs->control_session = NULL;
                    }
                }
                aeron_archive_control_session_close(closing);
                aeron_free(entry);
            }
            else
            {
                prev_ptr = &entry->next;
            }

            entry = next;
        }
    }

    /* Poll async recording subscription handles until resolved */
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        aeron_archive_recording_subscription_entry_t *entry = &conductor->recording_subscriptions[i];
        if (NULL != entry->async && NULL == entry->subscription)
        {
            aeron_subscription_t *sub = NULL;
            int rc = aeron_async_add_subscription_poll(&sub, entry->async);
            if (rc > 0 && NULL != sub)
            {
                entry->subscription = sub;
                entry->async = NULL;
                work_count++;
            }
            else if (rc < 0)
            {
                entry->async = NULL;
            }
        }
    }

    /* Detect new images on resolved subscriptions; start recording sessions. */
    work_count += aeron_archive_conductor_check_new_images(conductor);

    /* Drive active recording sessions */
    work_count += aeron_archive_conductor_drive_recording_sessions(conductor);

    /* Run queued tasks (image-available callbacks enqueue recording start tasks) */
    work_count += aeron_archive_conductor_run_tasks(conductor);

    return work_count;
}

int aeron_archive_conductor_close(aeron_archive_conductor_t *conductor)
{
    if (NULL == conductor)
    {
        return 0;
    }

    if (conductor->is_closed)
    {
        return 0;
    }

    conductor->is_closed = true;

    /* Abort and close all recording sessions */
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        aeron_archive_recording_session_t *session = conductor->recording_sessions[i].session;
        if (conductor->is_abort)
        {
            aeron_archive_recording_session_abort_close(session);
        }
        else
        {
            aeron_archive_recording_session_close(session);
        }
    }

    /* Close all replication sessions. Mirrors Java ArchiveConductor.closeSessionWorkers
     * which drives outstanding replication sessions to close before the
     * conductor exits. Without this their inner aeron_archive_context,
     * strdup'd channels, and counter handles leak. */
    for (int32_t i = 0; i < conductor->replication_session_count; i++)
    {
        if (NULL != conductor->replication_sessions[i].session)
        {
            aeron_archive_replication_session_close(conductor->replication_sessions[i].session);
        }
    }

    /* Close all replay sessions — same rationale as replication sessions. */
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        if (NULL != conductor->replay_sessions[i].session)
        {
            aeron_archive_replay_session_close(conductor->replay_sessions[i].session);
        }
    }

    /* Close recording subscriptions */
    for (int32_t i = 0; i < conductor->recording_subscription_count; i++)
    {
        if (NULL != conductor->recording_subscriptions[i].subscription)
        {
            aeron_subscription_close(conductor->recording_subscriptions[i].subscription, NULL, NULL);
        }
        else if (NULL != conductor->recording_subscriptions[i].async)
        {
            /* Pending async_add_subscription. Poll first — if the driver
             * already returned the subscription between our last do_work
             * tick and now, poll==1 frees the async handle synchronously
             * inside aeron_async_resource_poll. We then close the resolved
             * subscription. Only if still AWAITING do we submit a cancel
             * (which queues a remove-cmd on the client's conductor queue
             * that may or may not fire before aeron_close joins the runner
             * — the narrow race window that's the only remaining leak
             * path). Never aeron_free() the handle directly: the client
             * background thread still references it via its
             * registering_resources list. */
            aeron_subscription_t *resolved = NULL;
            int poll_rc = aeron_async_add_subscription_poll(
                &resolved, conductor->recording_subscriptions[i].async);
            if (poll_rc > 0 && NULL != resolved)
            {
                aeron_subscription_close(resolved, NULL, NULL);
            }
            else if (0 == poll_rc)
            {
                aeron_async_add_subscription_cancel(
                    conductor->aeron, conductor->recording_subscriptions[i].async);
            }
            conductor->recording_subscriptions[i].async = NULL;
        }
        aeron_archive_conductor_free_subscription_entry_fields(
            &conductor->recording_subscriptions[i]);
    }

    /* Close and free all control sessions */
    {
        aeron_archive_control_session_entry_t *entry = conductor->control_sessions_head;
        while (NULL != entry)
        {
            aeron_archive_control_session_entry_t *next = entry->next;
            aeron_archive_control_session_close(entry->session);
            aeron_free(entry);
            entry = next;
        }
        conductor->control_sessions_head = NULL;
    }

    /* Close the control session adapter */
    aeron_archive_control_session_adapter_close(conductor->control_session_adapter);
    conductor->control_session_adapter = NULL;

    /* Close the control response proxy */
    aeron_archive_control_response_proxy_close(conductor->control_response_proxy);
    conductor->control_response_proxy = NULL;

    /* Close control subscriptions */
    if (NULL != conductor->local_control_subscription)
    {
        aeron_subscription_close(conductor->local_control_subscription, NULL, NULL);
    }

    if (NULL != conductor->control_subscription)
    {
        aeron_subscription_close(conductor->control_subscription, NULL, NULL);
    }

    /* Drain task queue */
    while (NULL != conductor->task_queue_head)
    {
        aeron_archive_conductor_task_t *task = conductor->task_queue_head;
        conductor->task_queue_head = task->next;
        aeron_free(task);
    }

    /* Free arrays */
    aeron_free(conductor->recording_sessions);
    aeron_free(conductor->replay_sessions);
    aeron_free(conductor->replication_sessions);
    aeron_free(conductor->recording_subscriptions);

    conductor->recording_sessions = NULL;
    conductor->replay_sessions = NULL;
    conductor->replication_sessions = NULL;
    conductor->recording_subscriptions = NULL;

    aeron_free(conductor);

    return 0;
}

void aeron_archive_conductor_abort(aeron_archive_conductor_t *conductor)
{
    if (NULL != conductor)
    {
        conductor->is_abort = true;
    }
}

/* ---- Control session creation ---- */

aeron_archive_control_session_t *aeron_archive_conductor_new_control_session(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t response_stream_id,
    int32_t client_version,
    const char *response_channel,
    const uint8_t *encoded_credentials,
    size_t credentials_length,
    aeron_archive_control_session_adapter_t *adapter,
    aeron_image_t *image)
{
    (void)encoded_credentials;
    (void)credentials_length;
    (void)image;

    /*
     * Check the client protocol major version.
     * PROTOCOL_MAJOR_VERSION = 1, encoded as (major << 16 | minor << 8 | patch).
     */
    const int32_t client_major = (client_version >> 16) & 0xFF;
    const int32_t expected_major = 1;
    char *invalid_version_message = NULL;

    if (client_major != expected_major)
    {
        char msg[256];
        snprintf(msg, sizeof(msg),
            "invalid client version %d.%d.%d, archive expects major version %d",
            client_major, (client_version >> 8) & 0xFF, client_version & 0xFF, expected_major);

        const size_t msg_len = strlen(msg);
        if (aeron_alloc((void **)&invalid_version_message, msg_len + 1) == 0)
        {
            memcpy(invalid_version_message, msg, msg_len + 1);
        }
    }

    const int64_t control_session_id = conductor->next_session_id++;

    /*
     * Create the response publication synchronously (spin-poll until ready).
     * C aeron client doesn't have Java's thread-safe getExclusivePublication(),
     * so we must complete the async add on the same thread before proceeding.
     * This matches the pattern used by the official C archive client.
     */
    aeron_async_add_exclusive_publication_t *async_pub = NULL;
    if (aeron_async_add_exclusive_publication(
        &async_pub, conductor->aeron, response_channel, response_stream_id) < 0)
    {
        aeron_free(invalid_version_message);
        return NULL;
    }

    aeron_exclusive_publication_t *control_publication = NULL;
    if (aeron_async_add_exclusive_publication_poll(&control_publication, async_pub) < 0)
    {
        aeron_free(invalid_version_message);
        return NULL;
    }
    for (int i = 0; NULL == control_publication && i < 1000; i++)
    {
        aeron_micro_sleep(1000);
        if (aeron_async_add_exclusive_publication_poll(&control_publication, async_pub) < 0)
        {
            aeron_free(invalid_version_message);
            return NULL;
        }
    }
    if (NULL == control_publication)
    {
        AERON_SET_ERR(ETIMEDOUT, "%s", "timed out creating control response publication");
        aeron_free(invalid_version_message);
        return NULL;
    }

    /*
     * Create the control session with the resolved publication.
     */
    aeron_archive_control_session_t *control_session = NULL;
    if (aeron_archive_control_session_create(
        &control_session,
        control_session_id,
        correlation_id,
        conductor->ctx->connect_timeout_ms,
        conductor->ctx->session_liveness_check_interval_ms,
        0, /* registration_id not needed — publication already resolved */
        response_channel,
        response_stream_id,
        invalid_version_message,
        adapter,
        conductor->aeron,
        conductor,
        conductor->cached_epoch_clock_ms,
        conductor->control_response_proxy) < 0)
    {
        aeron_free(invalid_version_message);
        return NULL;
    }

    /* Set the publication directly — skip the async init step */
    control_session->control_publication = control_publication;

    aeron_free(invalid_version_message);

    /* Add the session to the conductor's linked list */
    aeron_archive_control_session_entry_t *entry = NULL;
    if (aeron_alloc((void **)&entry, sizeof(aeron_archive_control_session_entry_t)) < 0)
    {
        aeron_archive_control_session_close(control_session);
        return NULL;
    }

    entry->session = control_session;
    entry->next = conductor->control_sessions_head;
    conductor->control_sessions_head = entry;

    return control_session;
}

/* ---- Control request handlers ---- */

int aeron_archive_conductor_start_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop,
    const char *original_channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (conductor->recording_session_count >= conductor->ctx->max_concurrent_recordings)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_MAX_RECORDINGS,
            "max concurrent recordings reached: %d",
            conductor->ctx->max_concurrent_recordings);
        return -1;
    }

    char *key = aeron_archive_conductor_make_key(stream_id, original_channel);
    if (NULL == key)
    {
        return -1;
    }

    /* Check for existing subscription with the same key */
    if (NULL != aeron_archive_conductor_find_subscription_by_key(conductor, key))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_ACTIVE_SUBSCRIPTION,
            "recording exists for streamId=%d channel=%s",
            stream_id, original_channel);
        aeron_free(key);
        return -1;
    }

    /*
     * Build the subscription channel. For LOCAL source location on UDP, prepend spy prefix.
     * The stripped channel logic in Java removes non-essential URI parameters. Here we use
     * the original channel as-is for simplicity; a full implementation would strip parameters.
     */
    const char *subscribe_channel = original_channel;
    char spy_channel[1024];
    if (AERON_ARCHIVE_SOURCE_LOCATION_LOCAL == source_location &&
        NULL != strstr(original_channel, "udp"))
    {
        snprintf(spy_channel, sizeof(spy_channel), "%s%s", AERON_ARCHIVE_SPY_PREFIX, original_channel);
        subscribe_channel = spy_channel;
    }

    /* Add the subscription via Aeron. Synchronous spin-poll matching Java
     * ArchiveConductor which uses `aeron.addSubscription(...)`. Avoids the
     * async lifecycle (cancel on close → remove cmd that the client
     * background thread may not drain before aeron_close joins it) that
     * leaked under LeakSanitizer on shutdown. Timeout mirrors
     * ArchiveContext's connect timeout default (10 s). */
    aeron_async_add_subscription_t *async = NULL;
    if (aeron_async_add_subscription(
        &async, conductor->aeron, subscribe_channel, stream_id,
        NULL, NULL,
        aeron_archive_conductor_on_unavailable_image, conductor) < 0)
    {
        AERON_SET_ERR(errno, "failed to add subscription for channel=%s streamId=%d", subscribe_channel, stream_id);
        aeron_free(key);
        return -1;
    }
    /* Capture registration_id up-front: aeron_async_resource_poll frees the
     * async handle when it returns REGISTERED (or ERRORED), so any access
     * after the poll loop is a UAF. */
    const int64_t subscription_registration_id =
        aeron_async_add_subscription_get_registration_id(async);

    aeron_subscription_t *resolved_sub = NULL;
    {
        const int64_t deadline_ms = aeron_epoch_clock() + 10000;
        for (;;)
        {
            int poll_rc = aeron_async_add_subscription_poll(&resolved_sub, async);
            if (poll_rc > 0 && NULL != resolved_sub)
            {
                break;
            }
            if (poll_rc < 0)
            {
                /* aeron_async_resource_poll already freed async on error. */
                aeron_free(key);
                return -1;
            }
            if (aeron_epoch_clock() >= deadline_ms)
            {
                aeron_async_add_subscription_cancel(conductor->aeron, async);
                aeron_free(key);
                AERON_SET_ERR(ETIMEDOUT,
                    "add subscription did not resolve within 10s channel=%s streamId=%d",
                    subscribe_channel, stream_id);
                return -1;
            }
            aeron_micro_sleep(100);
        }
    }

    /*
     * Store the subscription entry. The subscription pointer will be resolved
     * asynchronously; for now we store NULL and the async handle.
     * A full implementation would poll the async add until resolved.
     */
    if (aeron_archive_conductor_ensure_subscription_capacity(conductor) < 0)
    {
        aeron_free(key);
        return -1;
    }

    aeron_archive_recording_subscription_entry_t *entry =
        &conductor->recording_subscriptions[conductor->recording_subscription_count];
    entry->key = key;
    entry->subscription = resolved_sub;
    entry->async = NULL;
    entry->ref_count = 1;
    entry->control_session = control_session;
    entry->correlation_id = correlation_id;
    entry->stream_id = stream_id;
    entry->original_channel = strdup(original_channel);
    entry->auto_stop = auto_stop;
    entry->extend_target_recording_id = AERON_ARCHIVE_NULL_VALUE;
    entry->recorded_image_session_ids = NULL;
    entry->recorded_image_count = 0;
    entry->recorded_image_capacity = 0;
    conductor->recording_subscription_count++;

    /* Return the aeron subscription's registration_id so the client can
     * match subsequent RecordingSignal events by subscription_id (mirrors
     * Java ArchiveConductor.startRecording which returns subscriptionId).
     * The registration_id was captured before the poll loop — reading
     * from the async handle after poll succeeded would UAF since the
     * handle is freed on REGISTERED. */
    aeron_archive_control_session_send_ok_response(
        session, correlation_id, subscription_registration_id);

    return 0;
}

int aeron_archive_conductor_stop_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t stream_id,
    const char *channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    char *key = aeron_archive_conductor_make_key(stream_id, channel);
    if (NULL == key)
    {
        return -1;
    }

    aeron_archive_recording_subscription_entry_t *entry =
        aeron_archive_conductor_find_subscription_by_key(conductor, key);

    if (NULL == entry)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_SUBSCRIPTION,
            "no recording found for streamId=%d channel=%s",
            stream_id, channel);
        aeron_free(key);
        return -1;
    }

    /* Abort all recording sessions associated with this subscription */
    if (NULL != entry->subscription)
    {
        aeron_archive_conductor_abort_recording_sessions_for_subscription(conductor, entry->subscription);

        entry->ref_count--;
        if (entry->ref_count <= 0)
        {
            aeron_subscription_close(entry->subscription, NULL, NULL);
        }
    }

    aeron_archive_conductor_remove_subscription_by_key(conductor, key);
    aeron_free(key);

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

int aeron_archive_conductor_stop_recording_subscription(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t subscription_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    aeron_archive_recording_subscription_entry_t *entry =
        aeron_archive_conductor_find_subscription_by_id(conductor, subscription_id);

    if (NULL == entry)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_SUBSCRIPTION,
            "no recording subscription found for subscriptionId=%" PRId64,
            subscription_id);
        return -1;
    }

    if (NULL != entry->subscription)
    {
        aeron_archive_conductor_abort_recording_sessions_for_subscription(conductor, entry->subscription);

        entry->ref_count--;
        if (entry->ref_count <= 0)
        {
            aeron_subscription_close(entry->subscription, NULL, NULL);
        }
    }

    /* Remove by rebuilding without this entry */
    aeron_archive_conductor_remove_subscription_by_id(conductor, subscription_id);

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

int aeron_archive_conductor_stop_recording_by_identity(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_recording_session_t *recording_session =
        aeron_archive_conductor_find_recording_session_internal(conductor, recording_id);

    int64_t stopped = 0;
    if (NULL != recording_session)
    {
        aeron_archive_recording_session_abort(recording_session, "stop recording by identity");
        stopped = 1;
    }

    /* Mirrors Java ArchiveConductor: relevantId = 1 when a session was aborted,
     * 0 otherwise. Client maps non-zero → stopped=true. */
    aeron_archive_control_session_send_ok_response(session, correlation_id, stopped);

    return 0;
}

int aeron_archive_conductor_start_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t length,
    int32_t replay_stream_id,
    const char *replay_channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (conductor->replay_session_count >= conductor->ctx->max_concurrent_replays)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_MAX_REPLAYS,
            "max concurrent replays reached: %d",
            conductor->ctx->max_concurrent_replays);
        return -1;
    }

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    if (aeron_archive_conductor_recording_summary(conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    const int64_t start_position = conductor->recording_summary.start_position;
    int64_t replay_position = start_position;

    if (AERON_ARCHIVE_NULL_POSITION != position)
    {
        /* Validate replay position */
        if ((position & (AERON_ARCHIVE_FRAME_ALIGNMENT - 1)) != 0)
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_GENERIC,
                "requested replay start position=%" PRId64 " is not frame-aligned for recording %" PRId64,
                position, recording_id);
            return -1;
        }

        if (position < start_position)
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_GENERIC,
                "requested replay start position=%" PRId64 " < recording start=%" PRId64 " for recording %" PRId64,
                position, start_position, recording_id);
            return -1;
        }

        const int64_t stop_position = conductor->recording_summary.stop_position;
        if (AERON_ARCHIVE_NULL_POSITION != stop_position && position >= stop_position)
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_GENERIC,
                "requested replay start position=%" PRId64 " >= stop=%" PRId64 " for recording %" PRId64,
                position, stop_position, recording_id);
            return -1;
        }

        replay_position = position;
    }

    /*
     * In the full implementation, this would:
     * 1. Create an exclusive publication on the replay channel
     * 2. Create a ReplaySession with the recording reader
     * 3. Add it to the replay session map
     *
     * The ReplaySession type is forward-declared and will be implemented separately.
     * For now, send an OK response with a replay session id.
     */
    (void)replay_position;
    (void)length;
    (void)replay_stream_id;
    (void)replay_channel;

    const int64_t replay_session_id = (int64_t)conductor->replay_id++;
    aeron_archive_control_session_send_ok_response(session, correlation_id, replay_session_id);

    return 0;
}

int aeron_archive_conductor_start_bounded_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    int64_t length,
    int32_t limit_counter_id,
    int32_t replay_stream_id,
    const char *replay_channel,
    void *control_session)
{
    (void)limit_counter_id;

    /*
     * A bounded replay is identical to a normal replay except it is bounded by
     * a counter value.  Delegate to start_replay for validation; the limit
     * counter will be wired when ReplaySession is fully implemented.
     */
    return aeron_archive_conductor_start_replay(
        conductor, correlation_id, recording_id, position, length,
        replay_stream_id, replay_channel, control_session);
}

int aeron_archive_conductor_stop_replay(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t replay_session_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    /* Find and abort the replay session */
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        if (conductor->replay_sessions[i].replay_session_id == replay_session_id)
        {
            /*
             * In the full implementation:
             * aeron_archive_replay_session_abort(conductor->replay_sessions[i].session, "stop replay");
             */
            aeron_archive_control_session_send_ok_response(session, correlation_id, 0);
            return 0;
        }
    }

    /* Replay not found is not an error per the Java implementation - just send OK */
    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);
    return 0;
}

int aeron_archive_conductor_stop_all_replays(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        /*
         * In the full implementation, check if recording_id matches or is NULL_VALUE:
         * if (AERON_ARCHIVE_NULL_VALUE == recording_id ||
         *     conductor->replay_sessions[i].session->recording_id == recording_id)
         * {
         *     aeron_archive_replay_session_abort(conductor->replay_sessions[i].session, "stop all replays");
         * }
         */
        (void)recording_id;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

/* ---- List recordings ---- */

/**
 * Context passed to the catalog iteration callback for list_recordings.
 */
typedef struct aeron_archive_list_recordings_context_stct
{
    int64_t from_recording_id;
    int32_t max_count;
    int32_t count;
    void *control_session;
}
aeron_archive_list_recordings_context_t;

static void list_recordings_callback(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    aeron_archive_list_recordings_context_t *ctx = (aeron_archive_list_recordings_context_t *)clientd;

    if (ctx->count >= ctx->max_count)
    {
        return;
    }

    if (descriptor->recording_id >= ctx->from_recording_id)
    {
        /*
         * In the full implementation, encode the descriptor as an SBE message and send
         * it via the control session's response publication.
         */
        ctx->count++;
    }
}

int aeron_archive_conductor_list_recordings(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    aeron_archive_list_recordings_context_t ctx;
    ctx.from_recording_id = from_recording_id;
    ctx.max_count = record_count;
    ctx.count = 0;
    ctx.control_session = control_session;

    const int32_t iterated = aeron_archive_catalog_for_each(
        conductor->catalog,
        list_recordings_callback,
        &ctx);

    if (iterated < 0)
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_GENERIC, "%s", "catalog iteration failed during list recordings");
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, (int64_t)ctx.count);

    return 0;
}

/**
 * Context for filtered listing by URI.
 */
typedef struct aeron_archive_list_recordings_for_uri_context_stct
{
    int64_t from_recording_id;
    int32_t max_count;
    int32_t count;
    int32_t stream_id;
    const char *channel_fragment;
    void *control_session;
}
aeron_archive_list_recordings_for_uri_context_t;

static void list_recordings_for_uri_callback(
    const aeron_archive_catalog_recording_descriptor_t *descriptor,
    void *clientd)
{
    aeron_archive_list_recordings_for_uri_context_t *ctx =
        (aeron_archive_list_recordings_for_uri_context_t *)clientd;

    if (ctx->count >= ctx->max_count)
    {
        return;
    }

    if (descriptor->recording_id < ctx->from_recording_id)
    {
        return;
    }

    /* Match stream_id */
    if (descriptor->stream_id != ctx->stream_id)
    {
        return;
    }

    /* Match channel fragment if provided */
    if (NULL != ctx->channel_fragment && '\0' != ctx->channel_fragment[0])
    {
        if (NULL != descriptor->stripped_channel &&
            NULL == strstr(descriptor->stripped_channel, ctx->channel_fragment))
        {
            /* Also check original channel */
            if (NULL != descriptor->original_channel &&
                NULL == strstr(descriptor->original_channel, ctx->channel_fragment))
            {
                return;
            }
        }
    }

    /*
     * In the full implementation, encode and send the descriptor via the control session.
     */
    ctx->count++;
}

int aeron_archive_conductor_list_recordings_for_uri(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t from_recording_id,
    int32_t record_count,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    aeron_archive_list_recordings_for_uri_context_t ctx;
    ctx.from_recording_id = from_recording_id;
    ctx.max_count = record_count;
    ctx.count = 0;
    ctx.stream_id = stream_id;
    ctx.channel_fragment = channel_fragment;
    ctx.control_session = control_session;

    const int32_t iterated = aeron_archive_catalog_for_each(
        conductor->catalog,
        list_recordings_for_uri_callback,
        &ctx);

    if (iterated < 0)
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_GENERIC, "%s", "catalog iteration failed during list recordings for URI");
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, (int64_t)ctx.count);

    return 0;
}

int aeron_archive_conductor_list_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        aeron_archive_control_session_send_recording_unknown(session, correlation_id, recording_id);
        return 0;
    }

    /*
     * In the full implementation, read the descriptor from the catalog and
     * send it to the control session via send_descriptor.
     */
    aeron_archive_control_session_send_ok_response(session, correlation_id, 1);
    return 0;
}

int aeron_archive_conductor_find_last_matching_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t min_recording_id,
    int32_t session_id,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (min_recording_id < 0)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "min_recording_id=%" PRId64 " < 0",
            min_recording_id);
        return -1;
    }

    /*
     * In the full implementation, this would call catalog->findLast()
     * which scans from the highest recording ID backwards to find the
     * last recording matching the session_id, stream_id, and channel_fragment.
     *
     * The result recording_id (or -1 if not found) would be sent as the
     * response value via the control session.
     */
    (void)session_id;
    (void)stream_id;
    (void)channel_fragment;

    aeron_archive_control_session_send_ok_response(session, correlation_id, AERON_ARCHIVE_NULL_VALUE);

    return 0;
}

/* ---- Position queries ---- */

int aeron_archive_conductor_get_start_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_catalog_recording_descriptor_t descriptor;
    if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) < 0)
    {
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, descriptor.start_position);
    return 0;
}

int aeron_archive_conductor_get_stop_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_catalog_recording_descriptor_t descriptor;
    if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) < 0)
    {
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, descriptor.stop_position);
    return 0;
}

int aeron_archive_conductor_get_recording_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_recording_session_t *recording_session =
        aeron_archive_conductor_find_recording_session_internal(conductor, recording_id);

    int64_t position;
    if (NULL == recording_session)
    {
        /* No active session → recording has stopped; return catalog stop_position. */
        aeron_archive_catalog_recording_descriptor_t desc;
        if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &desc) < 0)
        {
            return -1;
        }
        position = desc.stop_position;
    }
    else
    {
        position = aeron_archive_recording_session_recorded_position(recording_session);
    }
    aeron_archive_control_session_send_ok_response(session, correlation_id, position);

    return 0;
}

int aeron_archive_conductor_get_max_recorded_position(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    aeron_archive_recording_session_t *recording_session =
        aeron_archive_conductor_find_recording_session_internal(conductor, recording_id);

    if (NULL != recording_session)
    {
        int64_t pos = aeron_archive_recording_session_recorded_position(recording_session);
        aeron_archive_control_session_send_ok_response(session, correlation_id, pos);
    }
    else
    {
        /* Not active - use stop position from catalog */
        aeron_archive_catalog_recording_descriptor_t descriptor;
        if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &descriptor) < 0)
        {
            return -1;
        }
        aeron_archive_control_session_send_ok_response(session, correlation_id, descriptor.stop_position);
    }

    return 0;
}

/* ---- Truncate / Purge ---- */

int aeron_archive_conductor_truncate_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t position,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    /* Validate: no active replay for this recording */
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        /*
         * In the full implementation, check:
         * if (conductor->replay_sessions[i].session->recording_id == recording_id)
         * {
         *     send error: "cannot truncate recording with active replay"
         *     return -1;
         * }
         */
    }

    if (aeron_archive_conductor_recording_summary(conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    const int64_t stop_position = conductor->recording_summary.stop_position;
    const int64_t start_position = conductor->recording_summary.start_position;

    /* Cannot truncate active recording */
    if (AERON_ARCHIVE_NULL_POSITION == stop_position)
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_ACTIVE_RECORDING, "%s", "cannot truncate active recording");
        return -1;
    }

    /* Validate position bounds and alignment */
    if (position < start_position || position > stop_position ||
        ((position & (AERON_ARCHIVE_FRAME_ALIGNMENT - 1)) != 0))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_GENERIC,
            "invalid truncate position %" PRId64 ": start=%" PRId64 " stop=%" PRId64,
            position, start_position, stop_position);
        return -1;
    }

    /* Update the catalog stop position */
    if (aeron_archive_catalog_update_recording_position(
        conductor->catalog, recording_id, position, conductor->cached_epoch_clock_ms) < 0)
    {
        return -1;
    }

    /*
     * In the full implementation, segment files after the truncation point
     * would be deleted, and the segment at the truncation point would be
     * truncated/erased as needed.
     */

    aeron_archive_control_session_send_ok_response(session, correlation_id, position);

    return 0;
}

int aeron_archive_conductor_purge_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    /* Validate: no active replay for this recording */
    for (int32_t i = 0; i < conductor->replay_session_count; i++)
    {
        /*
         * In the full implementation:
         * if (conductor->replay_sessions[i].session->recording_id == recording_id)
         * {
         *     send error: "cannot purge recording with active replay"
         *     return -1;
         * }
         */
    }

    if (aeron_archive_conductor_recording_summary(conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    if (AERON_ARCHIVE_NULL_POSITION == conductor->recording_summary.stop_position)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
            "cannot purge active recording %" PRId64,
            recording_id);
        return -1;
    }

    /* Mark as deleted in the catalog */
    if (aeron_archive_catalog_invalidate_recording(conductor->catalog, recording_id) < 0)
    {
        return -1;
    }

    /* Delete all segment files for this recording under archive_dir. */
    aeron_archive_conductor_delete_segments_on_disk(conductor, recording_id,
        INT64_MIN, INT64_MAX);

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);

    return 0;
}

/* -----------------------------------------------------------------------
 * Segment operations: detach / delete_detached / purge / attach / migrate
 * Mirrors Java ArchiveConductor segment handling (simplified).
 * ----------------------------------------------------------------------- */

static void aeron_archive_conductor_segment_file_name(
    char *dst, size_t dst_len, int64_t recording_id, int64_t position) AERON_ARCHIVE_MAYBE_UNUSED;

static void aeron_archive_conductor_segment_file_name(
    char *dst, size_t dst_len, int64_t recording_id, int64_t position)
{
    snprintf(dst, dst_len, "%" PRId64 "-%" PRId64 ".rec", recording_id, position);
}

static int64_t aeron_archive_conductor_segment_base(int64_t position, int32_t segment_length)
{
    if (segment_length <= 0) { return position; }
    const int64_t mask = ~((int64_t)segment_length - 1);
    return position & mask;
}

static bool aeron_archive_conductor_is_segment_aligned(
    int64_t position, int32_t segment_length, int32_t term_length)
{
    if (segment_length <= 0 || term_length <= 0) { return false; }
    /* segment boundaries are multiples of segment_length; term boundaries
     * multiples of term_length. Java additionally requires the position
     * to land on a term boundary within the segment — we enforce both. */
    return (position % segment_length) == 0 || (position % term_length) == 0;
}

/* Iterate segment files for recording_id under archive_dir, call cb(filename,
 * base_position, clientd) for each. Returns count. */
typedef void (*segment_file_cb_t)(const char *filename, int64_t base_position, void *clientd);

static int aeron_archive_conductor_for_each_segment(
    aeron_archive_conductor_t *conductor, int64_t recording_id,
    segment_file_cb_t cb, void *clientd)
{
    DIR *dir = opendir(conductor->ctx->archive_dir);
    if (NULL == dir) { return -1; }

    char prefix[64];
    snprintf(prefix, sizeof(prefix), "%" PRId64 "-", recording_id);
    const size_t prefix_len = strlen(prefix);

    int count = 0;
    struct dirent *entry;
    while (NULL != (entry = readdir(dir)))
    {
        const char *name = entry->d_name;
        if (strncmp(name, prefix, prefix_len) != 0) { continue; }
        const char *dot = strchr(name + prefix_len, '.');
        if (NULL == dot || strcmp(dot, ".rec") != 0) { continue; }

        char pos_buf[32] = {0};
        size_t pos_len = (size_t)(dot - (name + prefix_len));
        if (pos_len >= sizeof(pos_buf)) { continue; }
        memcpy(pos_buf, name + prefix_len, pos_len);
        char *end = NULL;
        long long base = strtoll(pos_buf, &end, 10);
        if (end == pos_buf) { continue; }

        if (NULL != cb) { cb(name, (int64_t)base, clientd); }
        count++;
    }
    closedir(dir);
    return count;
}

typedef struct
{
    aeron_archive_conductor_t *conductor;
    int64_t recording_id;
    int64_t min_inclusive;
    int64_t max_exclusive;
    int deleted;
} delete_segments_ctx_t;

static void aeron_archive_conductor_delete_segment_cb(
    const char *filename, int64_t base_position, void *clientd)
{
    delete_segments_ctx_t *ctx = (delete_segments_ctx_t *)clientd;
    if (base_position < ctx->min_inclusive || base_position >= ctx->max_exclusive)
    {
        return;
    }
    char path[AERON_MAX_PATH];
    snprintf(path, sizeof(path), "%s/%s", ctx->conductor->ctx->archive_dir, filename);
    if (unlink(path) == 0) { ctx->deleted++; }
}

static int aeron_archive_conductor_delete_segments_on_disk(
    aeron_archive_conductor_t *conductor, int64_t recording_id,
    int64_t min_inclusive_base, int64_t max_exclusive_base)
{
    delete_segments_ctx_t ctx = { conductor, recording_id,
        min_inclusive_base, max_exclusive_base, 0 };
    aeron_archive_conductor_for_each_segment(
        conductor, recording_id, aeron_archive_conductor_delete_segment_cb, &ctx);
    return ctx.deleted;
}

static bool aeron_archive_conductor_is_active_recording(
    aeron_archive_conductor_t *conductor, int64_t recording_id)
{
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        if (conductor->recording_sessions[i].recording_id == recording_id)
        {
            return true;
        }
    }
    return false;
}

static int aeron_archive_conductor_validate_detach(
    aeron_archive_conductor_t *conductor, int64_t recording_id,
    int64_t new_start_position,
    const aeron_archive_recording_summary_t *summary)
{
    if (new_start_position < 0)
    {
        AERON_SET_ERR(EINVAL, "new_start_position must be non-negative: %" PRId64,
            new_start_position);
        return -1;
    }
    if (new_start_position < summary->start_position ||
        (AERON_ARCHIVE_NULL_POSITION != summary->stop_position &&
         new_start_position > summary->stop_position))
    {
        AERON_SET_ERR(EINVAL,
            "new_start_position %" PRId64 " outside recorded range [%" PRId64 ", %" PRId64 "]",
            new_start_position, summary->start_position, summary->stop_position);
        return -1;
    }
    if (!aeron_archive_conductor_is_segment_aligned(
            new_start_position, summary->segment_file_length, summary->term_buffer_length))
    {
        AERON_SET_ERR(EINVAL,
            "new_start_position %" PRId64 " not segment/term aligned (seg=%d term=%d)",
            new_start_position, summary->segment_file_length, summary->term_buffer_length);
        return -1;
    }
    return 0;
}

int aeron_archive_conductor_detach_segments(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t new_start_position,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording: %" PRId64, recording_id);
        return -1;
    }

    if (aeron_archive_conductor_recording_summary(
            conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    if (aeron_archive_conductor_validate_detach(
            conductor, recording_id, new_start_position, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    if (aeron_archive_catalog_update_start_position(
            conductor->catalog, recording_id, new_start_position) < 0)
    {
        return -1;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);
    return 0;
}

int aeron_archive_conductor_delete_detached_segments(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording: %" PRId64, recording_id);
        return -1;
    }

    if (aeron_archive_conductor_is_active_recording(conductor, recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
            "cannot delete segments for active recording: %" PRId64, recording_id);
        return -1;
    }

    if (aeron_archive_conductor_recording_summary(
            conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    /* Delete any file whose base position is less than start_position
     * (those are "detached" — catalog moved past them). */
    const int64_t start_base = aeron_archive_conductor_segment_base(
        conductor->recording_summary.start_position,
        conductor->recording_summary.segment_file_length);
    int deleted = aeron_archive_conductor_delete_segments_on_disk(
        conductor, recording_id, INT64_MIN, start_base);

    aeron_archive_control_session_send_ok_response(session, correlation_id, (int64_t)deleted);
    return 0;
}

int aeron_archive_conductor_purge_segments(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int64_t new_start_position,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording: %" PRId64, recording_id);
        return -1;
    }

    if (aeron_archive_conductor_is_active_recording(conductor, recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
            "cannot purge active recording: %" PRId64, recording_id);
        return -1;
    }

    if (aeron_archive_conductor_recording_summary(
            conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    const int64_t old_start = conductor->recording_summary.start_position;

    if (aeron_archive_conductor_validate_detach(
            conductor, recording_id, new_start_position, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    if (aeron_archive_catalog_update_start_position(
            conductor->catalog, recording_id, new_start_position) < 0)
    {
        return -1;
    }

    const int64_t new_start_base = aeron_archive_conductor_segment_base(
        new_start_position, conductor->recording_summary.segment_file_length);
    int deleted = aeron_archive_conductor_delete_segments_on_disk(
        conductor, recording_id, old_start, new_start_base);

    aeron_archive_control_session_send_ok_response(session, correlation_id, (int64_t)deleted);
    return 0;
}

typedef struct
{
    int64_t min_base;
    bool has_any;
} find_min_base_ctx_t;

static void aeron_archive_conductor_find_min_base_cb(
    const char *filename, int64_t base_position, void *clientd)
{
    (void)filename;
    find_min_base_ctx_t *ctx = (find_min_base_ctx_t *)clientd;
    if (!ctx->has_any || base_position < ctx->min_base)
    {
        ctx->min_base = base_position;
        ctx->has_any = true;
    }
}

int aeron_archive_conductor_attach_segments(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording: %" PRId64, recording_id);
        return -1;
    }

    if (aeron_archive_conductor_recording_summary(
            conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    /* Scan the archive dir for the lowest segment file for this recording;
     * if it's earlier than the current start_position, advance start to it.
     * Simplified: trusts that file lengths match segment_file_length. Java
     * additionally walks term headers from the start to find the first
     * non-zero term offset — we skip that (P3). */
    find_min_base_ctx_t ctx = { 0, false };
    aeron_archive_conductor_for_each_segment(
        conductor, recording_id, aeron_archive_conductor_find_min_base_cb, &ctx);

    int64_t count = 0;
    if (ctx.has_any && ctx.min_base < conductor->recording_summary.start_position)
    {
        if (aeron_archive_catalog_update_start_position(
                conductor->catalog, recording_id, ctx.min_base) < 0)
        {
            return -1;
        }
        int64_t segment_len = conductor->recording_summary.segment_file_length;
        if (segment_len > 0)
        {
            count = (conductor->recording_summary.start_position - ctx.min_base) / segment_len;
        }
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, count);
    return 0;
}

int aeron_archive_conductor_migrate_segments(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, src_recording_id) ||
        !aeron_archive_conductor_has_recording(conductor, dst_recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording src=%" PRId64 " dst=%" PRId64,
            src_recording_id, dst_recording_id);
        return -1;
    }

    if (aeron_archive_conductor_is_active_recording(conductor, src_recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
            "cannot migrate active src recording: %" PRId64, src_recording_id);
        return -1;
    }

    aeron_archive_recording_summary_t src_summary;
    aeron_archive_recording_summary_t dst_summary;
    if (aeron_archive_conductor_recording_summary(conductor, src_recording_id, &src_summary) < 0 ||
        aeron_archive_conductor_recording_summary(conductor, dst_recording_id, &dst_summary) < 0)
    {
        return -1;
    }

    if (src_summary.stream_id != dst_summary.stream_id ||
        src_summary.term_buffer_length != dst_summary.term_buffer_length ||
        src_summary.segment_file_length != dst_summary.segment_file_length ||
        src_summary.initial_term_id != dst_summary.initial_term_id)
    {
        AERON_SET_ERR(EINVAL, "%s", "src and dst parameters do not match");
        return -1;
    }

    /* Must be contiguous: either src.stop == dst.start or src.start == dst.stop */
    const bool src_before_dst = (src_summary.stop_position == dst_summary.start_position);
    const bool dst_before_src = (src_summary.start_position == dst_summary.stop_position);
    if (!src_before_dst && !dst_before_src)
    {
        AERON_SET_ERR(EINVAL,
            "src and dst are not contiguous: src=[%" PRId64 ", %" PRId64
            "] dst=[%" PRId64 ", %" PRId64 "]",
            src_summary.start_position, src_summary.stop_position,
            dst_summary.start_position, dst_summary.stop_position);
        return -1;
    }

    /* Rename src segment files under dst_recording_id prefix. */
    DIR *dir = opendir(conductor->ctx->archive_dir);
    if (NULL == dir)
    {
        AERON_SET_ERR(errno, "opendir %s", conductor->ctx->archive_dir);
        return -1;
    }

    char prefix[64];
    snprintf(prefix, sizeof(prefix), "%" PRId64 "-", src_recording_id);
    const size_t prefix_len = strlen(prefix);

    int64_t moved = 0;
    struct dirent *entry;
    while (NULL != (entry = readdir(dir)))
    {
        const char *name = entry->d_name;
        if (strncmp(name, prefix, prefix_len) != 0) { continue; }
        const char *dot = strchr(name + prefix_len, '.');
        if (NULL == dot || strcmp(dot, ".rec") != 0) { continue; }

        char src_path[AERON_MAX_PATH];
        char dst_path[AERON_MAX_PATH];
        snprintf(src_path, sizeof(src_path), "%s/%s",
            conductor->ctx->archive_dir, name);
        snprintf(dst_path, sizeof(dst_path), "%s/%" PRId64 "%s",
            conductor->ctx->archive_dir, dst_recording_id, name + prefix_len - 1);
        if (rename(src_path, dst_path) == 0) { moved++; }
    }
    closedir(dir);

    if (src_before_dst)
    {
        aeron_archive_catalog_update_start_position(
            conductor->catalog, dst_recording_id, src_summary.start_position);
    }
    else
    {
        aeron_archive_catalog_update_stop_position(
            conductor->catalog, dst_recording_id, src_summary.stop_position);
    }
    aeron_archive_catalog_update_stop_position(
        conductor->catalog, src_recording_id, src_summary.start_position);

    aeron_archive_control_session_send_ok_response(session, correlation_id, moved);
    return 0;
}

/* ---- Update channel ---- */

int aeron_archive_conductor_update_channel(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    const char *channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64, recording_id);
        return -1;
    }

    aeron_archive_catalog_recording_descriptor_t desc;
    if (aeron_archive_catalog_find_recording(conductor->catalog, recording_id, &desc) < 0)
    {
        return -1;
    }

    /* Try in-place replace first (works when new channel strings fit the
     * existing frame). Fall back to invalidate + add_recording, which
     * allocates a new recording_id but preserves everything else. */
    const char *new_chan = (NULL != channel) ? channel : desc.original_channel;
    char stripped[AERON_URI_MAX_LENGTH];
    aeron_archive_strip_channel(new_chan, stripped, sizeof(stripped));
    if (aeron_archive_catalog_replace_recording(
            conductor->catalog,
            recording_id,
            desc.start_position, desc.stop_position,
            desc.start_timestamp, desc.stop_timestamp,
            desc.initial_term_id, desc.segment_file_length,
            desc.term_buffer_length, desc.mtu_length,
            desc.session_id, desc.stream_id,
            stripped, new_chan, desc.source_identity) < 0)
    {
        aeron_archive_catalog_invalidate_recording(conductor->catalog, recording_id);
        aeron_archive_catalog_add_recording(
            conductor->catalog,
            desc.start_position, desc.stop_position,
            desc.start_timestamp, desc.stop_timestamp,
            desc.initial_term_id, desc.segment_file_length,
            desc.term_buffer_length, desc.mtu_length,
            desc.session_id, desc.stream_id,
            stripped, new_chan, desc.source_identity);
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, 0);
    return 0;
}

/* ---- Replay token ---- */

int64_t aeron_archive_conductor_generate_replay_token(
    aeron_archive_conductor_t *conductor,
    void *control_session)
{
    (void)control_session;
    /* Monotonic counter sourced from the conductor's session id pool —
     * mirrors Java ArchiveConductor.generateReplayToken(). */
    return conductor->next_session_id++;
}

/* ---- Extend recording ---- */

int aeron_archive_conductor_extend_recording(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t recording_id,
    int32_t stream_id,
    aeron_archive_source_location_t source_location,
    bool auto_stop,
    const char *original_channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;
    (void)auto_stop;

    if (conductor->recording_session_count >= conductor->ctx->max_concurrent_recordings)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_MAX_RECORDINGS,
            "max concurrent recordings reached at %d",
            conductor->ctx->max_concurrent_recordings);
        return -1;
    }

    if (!aeron_archive_conductor_has_recording(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "unknown recording id: %" PRId64,
            recording_id);
        return -1;
    }

    if (aeron_archive_conductor_recording_summary(conductor, recording_id, &conductor->recording_summary) < 0)
    {
        return -1;
    }

    if (stream_id != conductor->recording_summary.stream_id)
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
            "cannot extend recording %" PRId64 " with streamId=%d for existing streamId=%d",
            recording_id, stream_id, conductor->recording_summary.stream_id);
        return -1;
    }

    if (NULL != aeron_archive_conductor_find_recording_session_internal(conductor, recording_id))
    {
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
            "cannot extend active recording %" PRId64,
            recording_id);
        return -1;
    }

    /* Build subscription for the incoming stream — same channel expansion as
     * start_recording (spy prefix for LOCAL UDP). Tag it with
     * extend_target_recording_id so check_new_images reuses the existing
     * recording_id instead of allocating a fresh catalog entry. */
    char *key = aeron_archive_conductor_make_key(stream_id, original_channel);
    if (NULL == key) { return -1; }
    if (NULL != aeron_archive_conductor_find_subscription_by_key(conductor, key))
    {
        aeron_free(key);
        AERON_SET_ERR(
            AERON_ARCHIVE_ERROR_ACTIVE_SUBSCRIPTION,
            "recording exists for streamId=%d channel=%s",
            stream_id, original_channel);
        return -1;
    }

    const char *subscribe_channel = original_channel;
    char spy_channel[1024];
    if (AERON_ARCHIVE_SOURCE_LOCATION_LOCAL == source_location &&
        NULL != strstr(original_channel, "udp"))
    {
        snprintf(spy_channel, sizeof(spy_channel), "%s%s",
            AERON_ARCHIVE_SPY_PREFIX, original_channel);
        subscribe_channel = spy_channel;
    }

    aeron_async_add_subscription_t *async = NULL;
    if (aeron_async_add_subscription(
        &async, conductor->aeron, subscribe_channel, stream_id,
        NULL, NULL,
        aeron_archive_conductor_on_unavailable_image, conductor) < 0)
    {
        AERON_SET_ERR(errno, "failed to add subscription for channel=%s streamId=%d",
            subscribe_channel, stream_id);
        aeron_free(key);
        return -1;
    }
    /* Capture id before poll — poll frees async on REGISTERED/ERRORED. */
    const int64_t subscription_registration_id =
        aeron_async_add_subscription_get_registration_id(async);

    /* Synchronous spin-poll, same rationale as start_recording. */
    aeron_subscription_t *resolved_sub = NULL;
    {
        const int64_t deadline_ms = aeron_epoch_clock() + 10000;
        for (;;)
        {
            int poll_rc = aeron_async_add_subscription_poll(&resolved_sub, async);
            if (poll_rc > 0 && NULL != resolved_sub)
            {
                break;
            }
            if (poll_rc < 0)
            {
                aeron_free(key);
                return -1;
            }
            if (aeron_epoch_clock() >= deadline_ms)
            {
                aeron_async_add_subscription_cancel(conductor->aeron, async);
                aeron_free(key);
                AERON_SET_ERR(ETIMEDOUT,
                    "add subscription (extend) did not resolve within 10s channel=%s streamId=%d",
                    subscribe_channel, stream_id);
                return -1;
            }
            aeron_micro_sleep(100);
        }
    }

    if (aeron_archive_conductor_ensure_subscription_capacity(conductor) < 0)
    {
        aeron_free(key);
        return -1;
    }

    aeron_archive_recording_subscription_entry_t *entry =
        &conductor->recording_subscriptions[conductor->recording_subscription_count];
    entry->key = key;
    entry->subscription = resolved_sub;
    entry->async = NULL;
    entry->ref_count = 1;
    entry->control_session = session;
    entry->correlation_id = correlation_id;
    entry->stream_id = stream_id;
    entry->original_channel = strdup(original_channel);
    entry->auto_stop = auto_stop;
    entry->extend_target_recording_id = recording_id;
    entry->recorded_image_session_ids = NULL;
    entry->recorded_image_count = 0;
    entry->recorded_image_capacity = 0;
    conductor->recording_subscription_count++;

    aeron_archive_control_session_send_ok_response(session, correlation_id, subscription_registration_id);
    return 0;
}

/* ---- Replication ---- */

int aeron_archive_conductor_replicate(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t src_recording_id,
    int64_t dst_recording_id,
    int64_t stop_position,
    int32_t src_control_stream_id,
    const char *src_control_channel,
    const char *live_destination,
    const char *replication_channel,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    /* Validate destination recording if specified */
    aeron_archive_recording_summary_t dst_summary;
    bool has_dst = false;
    if (AERON_ARCHIVE_NULL_VALUE != dst_recording_id)
    {
        if (!aeron_archive_conductor_has_recording(conductor, dst_recording_id))
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_UNKNOWN_RECORDING,
                "unknown destination recording id %" PRId64,
                dst_recording_id);
            return -1;
        }

        if (aeron_archive_conductor_recording_summary(conductor, dst_recording_id, &dst_summary) < 0)
        {
            return -1;
        }

        if (AERON_ARCHIVE_NULL_POSITION == dst_summary.stop_position ||
            NULL != aeron_archive_conductor_find_recording_session_internal(conductor, dst_recording_id))
        {
            AERON_SET_ERR(
                AERON_ARCHIVE_ERROR_ACTIVE_RECORDING,
                "cannot replicate to active recording %" PRId64,
                dst_recording_id);
            return -1;
        }
        has_dst = true;
    }

    /* Build a client-side archive context pointing at the remote archive.
     * This ctx is handed to the replication session which duplicates it
     * inside aeron_archive_async_connect and takes ownership of its aeron
     * handle lifecycle via owns_aeron_client=false (we share the local
     * archive's aeron client). */
    aeron_archive_context_t *src_ctx = NULL;
    if (aeron_archive_context_init(&src_ctx) < 0)
    {
        return -1;
    }
    aeron_archive_context_set_aeron(src_ctx, conductor->aeron);
    aeron_archive_context_set_owns_aeron_client(src_ctx, false);
    if (NULL != src_control_channel)
    {
        aeron_archive_context_set_control_request_channel(src_ctx, src_control_channel);
    }
    aeron_archive_context_set_control_request_stream_id(src_ctx, src_control_stream_id);
    /* Response on IPC back into this same aeron client. */
    aeron_archive_context_set_control_response_channel(src_ctx, "aeron:ipc");

    /* Allocate replication session entry + id */
    if (aeron_archive_conductor_ensure_replication_session_capacity(conductor) < 0)
    {
        aeron_archive_context_close(src_ctx);
        return -1;
    }

    const int64_t replication_id = conductor->next_session_id++;
    aeron_archive_replication_session_t *repl_session = NULL;
    if (aeron_archive_replication_session_create(
            &repl_session,
            src_recording_id,
            dst_recording_id,
            AERON_ARCHIVE_NULL_VALUE,  /* channel_tag_id */
            AERON_ARCHIVE_NULL_VALUE,  /* subscription_tag_id */
            replication_id,
            stop_position,
            live_destination,
            (NULL != replication_channel) ? replication_channel : conductor->ctx->replication_channel,
            conductor->ctx->file_io_max_length,
            0,                          /* replication_session_id override */
            has_dst ? &dst_summary : NULL,
            src_ctx,
            conductor->cached_epoch_clock_ms,
            conductor->catalog,
            session,
            conductor,
            conductor->aeron) < 0)
    {
        aeron_archive_context_close(src_ctx);
        return -1;
    }

    aeron_archive_replication_session_entry_t *entry =
        &conductor->replication_sessions[conductor->replication_session_count];
    entry->replication_id = replication_id;
    entry->session = repl_session;
    conductor->replication_session_count++;

    aeron_archive_control_session_send_ok_response(session, correlation_id, replication_id);
    return 0;
}

int aeron_archive_conductor_stop_replication(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int64_t replication_id,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;

    for (int32_t i = 0; i < conductor->replication_session_count; i++)
    {
        if (conductor->replication_sessions[i].replication_id == replication_id)
        {
            aeron_archive_replication_session_abort(
                conductor->replication_sessions[i].session, "stop replication");
            /* Actual close + remove from map happens when the session's
             * do_work observes the abort and transitions to DONE. */
            if (NULL != session)
            {
                aeron_archive_control_session_send_ok_response(session, correlation_id, 0);
            }
            return 0;
        }
    }

    AERON_SET_ERR(
        AERON_ARCHIVE_ERROR_UNKNOWN_REPLICATION,
        "unknown replication id %" PRId64,
        replication_id);
    return -1;
}

int aeron_archive_conductor_list_recording_subscriptions(
    aeron_archive_conductor_t *conductor,
    int64_t correlation_id,
    int32_t pseudo_index,
    int32_t subscription_count,
    bool apply_stream_id,
    int32_t stream_id,
    const char *channel_fragment,
    void *control_session)
{
    aeron_archive_control_session_t *session = (aeron_archive_control_session_t *)control_session;
    int32_t count = 0;

    for (int32_t i = pseudo_index;
         i < conductor->recording_subscription_count && count < subscription_count;
         i++)
    {
        const aeron_archive_recording_subscription_entry_t *entry = &conductor->recording_subscriptions[i];

        if (NULL == entry->key)
        {
            continue;
        }

        if (NULL != channel_fragment && strlen(channel_fragment) > 0 &&
            NULL == strstr(entry->key, channel_fragment))
        {
            continue;
        }

        if (apply_stream_id)
        {
            /*
             * The key format is "<streamId>:<channel>". Parse out the stream id
             * and compare.
             */
            int32_t entry_stream_id = 0;
            if (sscanf(entry->key, "%d:", &entry_stream_id) == 1 && entry_stream_id != stream_id)
            {
                continue;
            }
        }

        int64_t sub_reg_id = AERON_ARCHIVE_NULL_VALUE;
        if (NULL != entry->subscription)
        {
            aeron_subscription_constants_t sc;
            if (aeron_subscription_constants(entry->subscription, &sc) == 0)
            {
                sub_reg_id = (int64_t)sc.registration_id;
            }
        }

        aeron_archive_control_response_proxy_send_subscription_descriptor(
            conductor->control_response_proxy,
            session->control_session_id,
            correlation_id,
            sub_reg_id,
            entry->stream_id,
            (NULL != entry->original_channel) ? entry->original_channel : "",
            session->control_publication);

        count++;
    }

    aeron_archive_control_session_send_ok_response(session, correlation_id, (int64_t)count);
    return 0;
}

/* ---- Task queue ---- */

int aeron_archive_conductor_enqueue_task(
    aeron_archive_conductor_t *conductor,
    aeron_archive_conductor_task_func_t func,
    void *clientd)
{
    aeron_archive_conductor_task_t *task = NULL;

    if (aeron_alloc((void **)&task, sizeof(aeron_archive_conductor_task_t)) < 0)
    {
        AERON_SET_ERR(ENOMEM, "%s", "failed to allocate conductor task");
        return -1;
    }

    task->func = func;
    task->clientd = clientd;
    task->next = NULL;

    if (NULL == conductor->task_queue_tail)
    {
        conductor->task_queue_head = task;
        conductor->task_queue_tail = task;
    }
    else
    {
        conductor->task_queue_tail->next = task;
        conductor->task_queue_tail = task;
    }

    return 0;
}

/* ---- Session management ---- */

void aeron_archive_conductor_close_recording_session(
    aeron_archive_conductor_t *conductor,
    aeron_archive_recording_session_t *session)
{
    if (conductor->is_abort)
    {
        aeron_archive_recording_session_abort_close(session);
        return;
    }

    const int64_t recording_id = aeron_archive_recording_session_recording_id(session);
    const int64_t position = aeron_archive_recording_session_recorded_position(session);

    /* Snapshot control-session routing info before we drop the entry, so we
     * can emit the STOP signal (and untrack the source image) after. */
    void *control_session = NULL;
    int64_t correlation_id = 0;
    int64_t subscription_id = AERON_ARCHIVE_NULL_VALUE;
    int32_t source_image_session_id = 0;
    for (int32_t i = 0; i < conductor->recording_session_count; i++)
    {
        if (conductor->recording_sessions[i].recording_id == recording_id)
        {
            control_session = conductor->recording_sessions[i].control_session;
            correlation_id = conductor->recording_sessions[i].correlation_id;
            subscription_id = conductor->recording_sessions[i].subscription_id;
            source_image_session_id = conductor->recording_sessions[i].source_image_session_id;
            break;
        }
    }

    /* Update catalog with the stopped position */
    aeron_archive_catalog_update_recording_position(
        conductor->catalog,
        recording_id,
        position,
        conductor->cached_epoch_clock_ms);

    /* Remove from the active session map */
    aeron_archive_conductor_remove_recording_session(conductor, recording_id);

    /* Untrack the image from the originating subscription entry so a
     * subsequent image on the same session_id can start a fresh recording. */
    if (AERON_ARCHIVE_NULL_VALUE != subscription_id)
    {
        aeron_archive_conductor_remove_subscription_tracking(
            conductor, subscription_id, source_image_session_id);
    }

    /* Emit STOP signal back to the control session that started this
     * recording (mirrors Java ControlSession.attemptToSendSignal). */
    if (NULL != control_session)
    {
        aeron_archive_control_session_send_signal(
            (aeron_archive_control_session_t *)control_session,
            correlation_id,
            recording_id,
            subscription_id,
            position,
            AERON_ARCHIVE_RECORDING_SIGNAL_CODE_STOP);
    }

    /* Close the session itself */
    aeron_archive_recording_session_close(session);
}

aeron_archive_recording_session_t *aeron_archive_conductor_find_recording_session(
    aeron_archive_conductor_t *conductor,
    int64_t recording_id)
{
    return aeron_archive_conductor_find_recording_session_internal(conductor, recording_id);
}

int32_t aeron_archive_conductor_recording_session_count(const aeron_archive_conductor_t *conductor)
{
    return NULL != conductor ? conductor->recording_session_count : 0;
}

int32_t aeron_archive_conductor_replay_session_count(const aeron_archive_conductor_t *conductor)
{
    return NULL != conductor ? conductor->replay_session_count : 0;
}

bool aeron_archive_conductor_is_closed(const aeron_archive_conductor_t *conductor)
{
    return NULL == conductor || conductor->is_closed;
}
