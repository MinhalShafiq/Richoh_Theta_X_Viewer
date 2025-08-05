#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <time.h>
#include <sys/time.h>
#include <sys/stat.h>

#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <gst/app/gstappsink.h>
#include <libuvc/libuvc.h>

#define MAX_PIPELINE_LEN 1024
#define CAPTURE_RATE_HZ 5
#define CAPTURE_INTERVAL_US (1000000 / CAPTURE_RATE_HZ)  // 200ms in microseconds

// Global GStreamer source context
struct gst_src {
    GstElement *pipeline;
    GstElement *appsrc;
    GstElement *appsink;
    GMainLoop *loop;
    GTimer *timer;
    guint framecount;
    guint saved_count;
    guint bus_watch_id;
    uint32_t dwFrameInterval;
    FILE *csv_file;
    struct timeval last_capture;
    pthread_mutex_t capture_mutex;
    GstSample *latest_sample;  // Store latest decoded sample
    gboolean new_sample_available;
} src;

// Create output directory if it doesn't exist
void create_output_dir(const char *dirname) {
    struct stat st = {0};
    if (stat(dirname, &st) == -1) {
        mkdir(dirname, 0755);
        printf("Created directory: %s\n", dirname);
    }
}

// Get current timestamp as string
void get_timestamp_string(char *buffer, size_t size) {
    struct timeval tv;
    struct tm *tm_info;
    
    gettimeofday(&tv, NULL);
    tm_info = localtime(&tv.tv_sec);
    
    snprintf(buffer, size, "%04d%02d%02d_%02d%02d%02d_%06ld",
             tm_info->tm_year + 1900, tm_info->tm_mon + 1, tm_info->tm_mday,
             tm_info->tm_hour, tm_info->tm_min, tm_info->tm_sec,
             tv.tv_usec);
}

// Get current timestamp in seconds with microsecond precision
double get_timestamp_seconds() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Check if it's time to capture (5Hz rate limiting) - DEPRECATED
// This function is no longer used as we now use a GLib timer
gboolean should_capture() {
    struct timeval now;
    gettimeofday(&now, NULL);
    
    long diff_us = (now.tv_sec - src.last_capture.tv_sec) * 1000000 + 
                   (now.tv_usec - src.last_capture.tv_usec);
    
    if (diff_us >= CAPTURE_INTERVAL_US) {
        src.last_capture = now;
        return TRUE;
    }
    return FALSE;
}

// Save frame as JPEG
void save_frame_as_jpeg(GstSample *sample) {
    GstBuffer *buffer;
    GstMapInfo map;
    char filename[256];
    char timestamp[64];
    FILE *file;
    double ts_seconds;
    
    if (!sample) return;
    
    buffer = gst_sample_get_buffer(sample);
    if (!buffer) return;
    
    if (!gst_buffer_map(buffer, &map, GST_MAP_READ)) {
        g_printerr("Failed to map buffer for saving\n");
        return;
    }
    
    // Generate filename with timestamp
    get_timestamp_string(timestamp, sizeof(timestamp));
    snprintf(filename, sizeof(filename), "captures/frame_%s.jpg", timestamp);
    
    file = fopen(filename, "wb");
    if (file) {
        fwrite(map.data, 1, map.size, file);
        fclose(file);
        
        // Get precise timestamp for CSV
        ts_seconds = get_timestamp_seconds();
        
        // Write to CSV
        fprintf(src.csv_file, "frame_%s.jpg,%.6f\n", timestamp, ts_seconds);
        fflush(src.csv_file);  // Ensure data is written immediately
        
        src.saved_count++;
        printf("Saved frame %u: %s (%.6f)\n", src.saved_count, filename, ts_seconds);
    } else {
        g_printerr("Failed to open file: %s\n", filename);
    }
    
    gst_buffer_unmap(buffer, &map);
}

// Callback for appsink - stores latest decoded frame
GstFlowReturn on_new_sample(GstAppSink *appsink, gpointer user_data) {
    GstSample *sample;
    
    sample = gst_app_sink_pull_sample(appsink);
    if (!sample) {
        return GST_FLOW_OK;
    }
    
    pthread_mutex_lock(&src.capture_mutex);
    
    // Replace the previous sample with the latest one
    if (src.latest_sample) {
        gst_sample_unref(src.latest_sample);
    }
    
    src.latest_sample = sample;  // Take ownership
    src.new_sample_available = TRUE;
    
    pthread_mutex_unlock(&src.capture_mutex);
    
    return GST_FLOW_OK;
}

// Timer callback to capture at exactly 5Hz
gboolean capture_timer_callback(gpointer user_data) {
    pthread_mutex_lock(&src.capture_mutex);
    
    if (src.new_sample_available && src.latest_sample) {
        save_frame_as_jpeg(src.latest_sample);
        src.new_sample_available = FALSE;  // Mark as consumed
    }
    
    pthread_mutex_unlock(&src.capture_mutex);
    
    return TRUE;  // Continue timer
}

// GStreamer bus callback
static gboolean gst_bus_cb(GstBus *bus, GstMessage *message, gpointer data) {
    GError *err = NULL;
    gchar *dbg = NULL;

    switch (GST_MESSAGE_TYPE(message)) {
        case GST_MESSAGE_ERROR:
            gst_message_parse_error(message, &err, &dbg);
            g_printerr("GStreamer Error: %s\nDebug: %s\n", err->message, dbg);
            g_error_free(err);
            g_free(dbg);
            g_main_loop_quit(src.loop);
            break;
        case GST_MESSAGE_WARNING:
            gst_message_parse_warning(message, &err, &dbg);
            g_printerr("GStreamer Warning: %s\n", err->message);
            g_error_free(err);
            g_free(dbg);
            break;
        case GST_MESSAGE_EOS:
            g_print("End of stream\n");
            g_main_loop_quit(src.loop);
            break;
        default:
            break;
    }
    return TRUE;
}

// Initialize GStreamer pipeline with JPEG encoding and appsink
int gst_src_init(int *argc, char ***argv) {
    GstCaps *caps, *sink_caps;
    GstBus *bus;
    GstAppSinkCallbacks callbacks = {NULL};
    char pipeline_str[MAX_PIPELINE_LEN];
    guint timer_id;

    // Pipeline: appsrc -> h264parse -> decoder -> convert -> JPEG encoder -> appsink
    snprintf(pipeline_str, sizeof(pipeline_str),
             "appsrc name=ap is-live=true do-timestamp=true ! "
             "queue ! h264parse ! queue ! avdec_h264 ! "
             "videoconvert ! jpegenc quality=90 ! "
             "appsink name=sink emit-signals=true sync=false max-buffers=1 drop=true");

    gst_init(argc, argv);
    
    pthread_mutex_init(&src.capture_mutex, NULL);
    gettimeofday(&src.last_capture, NULL);  // Initialize capture timer
    src.latest_sample = NULL;
    src.new_sample_available = FALSE;

    src.timer = g_timer_new();
    src.loop = g_main_loop_new(NULL, FALSE);
    src.pipeline = gst_parse_launch(pipeline_str, NULL);

    if (!src.pipeline) {
        g_printerr("Failed to create GStreamer pipeline\n");
        return FALSE;
    }

    src.appsrc = gst_bin_get_by_name(GST_BIN(src.pipeline), "ap");
    src.appsink = gst_bin_get_by_name(GST_BIN(src.pipeline), "sink");
    
    if (!src.appsrc || !src.appsink) {
        g_printerr("Failed to get pipeline elements\n");
        return FALSE;
    }

    // Configure appsrc with H.264 caps
    caps = gst_caps_new_simple("video/x-h264",
        "stream-format", G_TYPE_STRING, "byte-stream",
        "alignment", G_TYPE_STRING, "au",
        "width", G_TYPE_INT, 1920,
        "height", G_TYPE_INT, 960,
        "framerate", GST_TYPE_FRACTION, 29, 1,
        "profile", G_TYPE_STRING, "constrained-baseline",
        NULL);

    gst_app_src_set_caps(GST_APP_SRC(src.appsrc), caps);
    gst_caps_unref(caps);

    // Configure appsink to drop old frames and keep only latest
    sink_caps = gst_caps_new_simple("image/jpeg", NULL);
    gst_app_sink_set_caps(GST_APP_SINK(src.appsink), sink_caps);
    gst_caps_unref(sink_caps);
    
    // Configure appsink to drop frames when buffer is full
    g_object_set(src.appsink, 
                 "max-buffers", 1,      // Keep only 1 buffer
                 "drop", TRUE,          // Drop old buffers when full
                 "sync", FALSE,         // Don't sync to clock
                 NULL);
    
    callbacks.new_sample = on_new_sample;
    gst_app_sink_set_callbacks(GST_APP_SINK(src.appsink), &callbacks, NULL, NULL);

    // Set up 5Hz timer (200ms intervals)
    timer_id = g_timeout_add(200, capture_timer_callback, NULL);

    bus = gst_pipeline_get_bus(GST_PIPELINE(src.pipeline));
    src.bus_watch_id = gst_bus_add_watch(bus, gst_bus_cb, NULL);
    gst_object_unref(bus);

    return TRUE;
}

// Thread to stop on keypress
void *keywait(void *arg) {
    getchar();  // Wait for Enter or any key
    g_main_loop_quit(src.loop);
    return NULL;
}

// UVC Callback: Push H.264 frame into GStreamer
void cb(uvc_frame_t *frame, void *ptr) {
    GstBuffer *buffer;
    GstFlowReturn ret;
    GstMapInfo map;

    if (frame->data_bytes == 0) return;

    buffer = gst_buffer_new_allocate(NULL, frame->data_bytes, NULL);
    if (!buffer) {
        fprintf(stderr, "Failed to allocate GstBuffer\n");
        return;
    }

    // Set timestamp
    GstClockTime ts = frame->sequence * (GstClockTime)src.dwFrameInterval * 100;
    GST_BUFFER_PTS(buffer) = ts;
    GST_BUFFER_DTS(buffer) = ts;
    GST_BUFFER_DURATION(buffer) = (GstClockTime)src.dwFrameInterval * 100;
    GST_BUFFER_OFFSET(buffer) = frame->sequence;

    // Copy data
    if (gst_buffer_map(buffer, &map, GST_MAP_WRITE)) {
        memcpy(map.data, frame->data, frame->data_bytes);
        gst_buffer_unmap(buffer, &map);
    } else {
        fprintf(stderr, "Failed to map buffer\n");
        gst_buffer_unref(buffer);
        return;
    }

    // Push to GStreamer
    g_signal_emit_by_name(src.appsrc, "push-buffer", buffer, &ret);
    gst_buffer_unref(buffer);

    if (ret != GST_FLOW_OK) {
        fprintf(stderr, "GStreamer push error: %d\n", ret);
        g_main_loop_quit(src.loop);
    }

    src.framecount++;
    if (src.framecount % 150 == 0) {  // Print every 5 seconds at 30fps
        g_print("Processed %u frames, saved %u images\n", src.framecount, src.saved_count);
    }
}

// Find ANY UVC device
uvc_error_t find_uvc_device(uvc_context_t *ctx, uvc_device_t **dev_out) {
    uvc_error_t res = uvc_find_device(ctx, dev_out, 0, 0, NULL);
    if (res != UVC_SUCCESS) {
        fprintf(stderr, "No UVC device found. Is camera plugged in?\n");
    } else {
        uvc_device_descriptor_t *desc;
        if (uvc_get_device_descriptor(*dev_out, &desc) == UVC_SUCCESS) {
            printf("Found device: %s (0x%04x:0x%04x)\n",
                   desc->product ? desc->product : "Unknown",
                   desc->idVendor, desc->idProduct);
            uvc_free_device_descriptor(desc);
        }
    }
    return res;
}

// Force known-good format for Yi 4K Plus
uvc_error_t set_h264_format(uvc_device_handle_t *devh, uvc_stream_ctrl_t *ctrl) {
    uvc_error_t res = uvc_get_stream_ctrl_format_size(
        devh, ctrl,
        UVC_FRAME_FORMAT_H264,
        1920, 960,     // Known supported resolution
        29             // Closest to 29.97 fps
    );

    if (res < 0) {
        fprintf(stderr, "Failed to set 1920x960 @ 29fps\n");
        uvc_perror(res, "uvc_get_stream_ctrl_format_size");
    } else {
        printf("Streaming: 1920x960 @ 29 fps\n");
    }

    return res;
}

// Print stream control info
void print_ctrl(uvc_stream_ctrl_t *ctrl) {
    printf("Stream Config:\n");
    printf("  Format: H.264\n");
    printf("  Frame Interval: %u (1/%.2f fps)\n",
           ctrl->dwFrameInterval, 10000000.0f / ctrl->dwFrameInterval);
    printf("  Max Frame Size: %u\n", ctrl->dwMaxVideoFrameSize);
    printf("  Max Payload: %u\n", ctrl->dwMaxPayloadTransferSize);
    printf("  Capture Rate: %d Hz (every %.1f ms)\n", CAPTURE_RATE_HZ, 1000.0/CAPTURE_RATE_HZ);
}

// Main
int main(int argc, char **argv) {
    uvc_context_t *ctx = NULL;
    uvc_device_t *dev = NULL;
    uvc_device_handle_t *devh = NULL;
    uvc_stream_ctrl_t ctrl;
    uvc_error_t res;
    char csv_filename[256];
    char timestamp[64];

    // Create output directory
    create_output_dir("captures");

    // Create CSV file with timestamp
    get_timestamp_string(timestamp, sizeof(timestamp));
    snprintf(csv_filename, sizeof(csv_filename), "captures/capture_log_%s.csv", timestamp);
    
    src.csv_file = fopen(csv_filename, "w");
    if (!src.csv_file) {
        fprintf(stderr, "Failed to create CSV file: %s\n", csv_filename);
        return -1;
    }
    
    // Write CSV header
    fprintf(src.csv_file, "filename,timestamp_seconds\n");
    printf("Created CSV log: %s\n", csv_filename);

    // Initialize UVC
    res = uvc_init(&ctx, NULL);
    if (res < 0) {
        uvc_perror(res, "uvc_init");
        goto close_csv;
    }
    printf("UVC initialized\n");

    // Find device
    res = find_uvc_device(ctx, &dev);
    if (res < 0) goto cleanup;

    // Open device
    res = uvc_open(dev, &devh);
    if (res < 0) {
        uvc_perror(res, "uvc_open");
        goto cleanup;
    }
    printf("Device opened\n");

    // Print diagnostics
    uvc_print_diag(devh, stderr);

    // Set H.264 format
    res = set_h264_format(devh, &ctrl);
    if (res < 0) goto close_device;

    print_ctrl(&ctrl);

    // Initialize GStreamer
    if (!gst_src_init(&argc, &argv)) {
        fprintf(stderr, "Failed to initialize GStreamer\n");
        goto close_device;
    }

    // Set timing
    src.dwFrameInterval = ctrl.dwFrameInterval;
    src.framecount = 0;
    src.saved_count = 0;

    // Start GStreamer
    gst_element_set_state(src.pipeline, GST_STATE_PLAYING);
    printf("GStreamer pipeline started\n");

    // Start UVC streaming
    res = uvc_start_streaming(devh, &ctrl, cb, NULL, 0);
    if (res < 0) {
        uvc_perror(res, "uvc_start_streaming");
        goto stop_pipeline;
    }

    printf("Streaming and capturing at %d Hz... Press ENTER to stop.\n", CAPTURE_RATE_HZ);

    // Start keypress thread
    pthread_t key_thread;
    pthread_create(&key_thread, NULL, keywait, NULL);

    // Run GStreamer loop
    g_main_loop_run(src.loop);

    // Stop
    uvc_stop_streaming(devh);
    printf("Streaming stopped. Saved %u images total.\n", src.saved_count);

    // Join thread
    pthread_join(key_thread, NULL);

stop_pipeline:
    gst_element_set_state(src.pipeline, GST_STATE_NULL);
    g_source_remove(src.bus_watch_id);
    g_main_loop_unref(src.loop);
    g_timer_destroy(src.timer);
    pthread_mutex_destroy(&src.capture_mutex);
    
    // Clean up latest sample
    if (src.latest_sample) {
        gst_sample_unref(src.latest_sample);
    }

close_device:
    uvc_close(devh);
    uvc_unref_device(dev);
    printf("Device closed.\n");

cleanup:
    uvc_exit(ctx);
    printf("UVC exited.\n");

close_csv:
    if (src.csv_file) {
        fclose(src.csv_file);
        printf("CSV file closed.\n");
    }

    return res;
}