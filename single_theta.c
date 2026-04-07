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
#define MAX_CAMERAS 2

// Structure to store discovered device info
typedef struct {
    uvc_device_t *dev;
    char serial[64];
} DiscoveredDevice;

// Camera instance structure
typedef struct {
    int camera_id;
    char serial[64];
    // UVC components
    uvc_device_t *dev;
    uvc_device_handle_t *devh;
    uvc_stream_ctrl_t ctrl;
    // GStreamer components
    GstElement *pipeline;
    GstElement *appsrc;
    GstElement *appsink;
    GTimer *timer;
    guint framecount;
    guint saved_count;
    guint bus_watch_id;
    uint32_t dwFrameInterval;
    // Capture management
    FILE *csv_file;
    pthread_mutex_t capture_mutex;
    GstSample *latest_sample;
    gboolean is_active;
    gboolean is_streaming;
    // Timer ID for capture
    guint timer_id;
    // Debug counters
    guint frames_received;
    guint samples_received;
    guint last_frame_count;
    // Thread for streaming
    pthread_t stream_thread;
} CameraInstance;

// Global context
struct {
    GMainLoop *loop;
    CameraInstance cameras[MAX_CAMERAS];
    int active_cameras;
    pthread_t key_thread;
    pthread_t monitor_thread;
    volatile int should_stop;
    uvc_context_t *uvc_ctx;  // Shared context for all cameras
} g_ctx;

// Create output directory
void create_output_dir(const char *dirname) {
    struct stat st = {0};
    if (stat(dirname, &st) == -1) {
        if (mkdir(dirname, 0755) != 0) {
            perror("Failed to create directory");
        } else {
            printf("Created directory: %s\n", dirname);
        }
    }
}

// Get timestamp string
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

// Get timestamp in seconds
double get_timestamp_seconds() {
    struct timeval tv;
    gettimeofday(&tv, NULL);
    return tv.tv_sec + tv.tv_usec / 1000000.0;
}

// Save frame as JPEG
void save_frame_as_jpeg(CameraInstance *cam, GstSample *sample) {
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
        g_printerr("Camera %d: Failed to map buffer\n", cam->camera_id);
        return;
    }

    get_timestamp_string(timestamp, sizeof(timestamp));
    snprintf(filename, sizeof(filename), "captures/cam%d_frame_%s.jpg", 
             cam->camera_id, timestamp);
    file = fopen(filename, "wb");
    if (file) {
        fwrite(map.data, 1, map.size, file);
        fclose(file);
        ts_seconds = get_timestamp_seconds();
        fprintf(cam->csv_file, "cam%d_frame_%s.jpg,%.6f\n", 
                cam->camera_id, timestamp, ts_seconds);
        fflush(cam->csv_file);
        cam->saved_count++;
        printf("Camera %d: Saved frame %u at %.6f\n", 
               cam->camera_id, cam->saved_count, ts_seconds);
    }
    gst_buffer_unmap(buffer, &map);
}

// Appsink callback
GstFlowReturn on_new_sample(GstAppSink *appsink, gpointer user_data) {
    CameraInstance *cam = (CameraInstance *)user_data;
    GstSample *sample;

    sample = gst_app_sink_pull_sample(appsink);
    if (!sample) {
        return GST_FLOW_OK;
    }

    cam->samples_received++;
    pthread_mutex_lock(&cam->capture_mutex);
    if (cam->latest_sample) {
        gst_sample_unref(cam->latest_sample);
    }
    cam->latest_sample = sample;
    pthread_mutex_unlock(&cam->capture_mutex);

    return GST_FLOW_OK;
}

// Timer callback for 5Hz capture
gboolean capture_timer_callback(gpointer user_data) {
    CameraInstance *cam = (CameraInstance *)user_data;

    if (!cam->is_active || !cam->is_streaming) {
        return TRUE;
    }

    pthread_mutex_lock(&cam->capture_mutex);
    if (cam->latest_sample) {
        save_frame_as_jpeg(cam, cam->latest_sample);
    } else {
        static int no_sample_count[MAX_CAMERAS] = {0};
        no_sample_count[cam->camera_id]++;
        if (no_sample_count[cam->camera_id] % 25 == 0) {
            printf("Camera %d: No decoded samples available (checked %d times)\n", 
                   cam->camera_id, no_sample_count[cam->camera_id]);
        }
    }
    pthread_mutex_unlock(&cam->capture_mutex);

    return TRUE;
}

// GStreamer bus callback
static gboolean gst_bus_cb(GstBus *bus, GstMessage *message, gpointer data) {
    CameraInstance *cam = (CameraInstance *)data;
    GError *err = NULL;
    gchar *dbg = NULL;

    switch (GST_MESSAGE_TYPE(message)) {
        case GST_MESSAGE_ERROR:
            gst_message_parse_error(message, &err, &dbg);
            g_printerr("Camera %d Pipeline Error: %s\n", cam->camera_id, err->message);
            if (dbg) g_printerr("Debug: %s\n", dbg);
            g_error_free(err);
            g_free(dbg);
            cam->is_active = FALSE;
            break;
        case GST_MESSAGE_WARNING:
            gst_message_parse_warning(message, &err, &dbg);
            g_printerr("Camera %d Pipeline Warning: %s\n", cam->camera_id, err->message);
            g_error_free(err);
            g_free(dbg);
            break;
        case GST_MESSAGE_STATE_CHANGED:
            if (GST_MESSAGE_SRC(message) == GST_OBJECT(cam->pipeline)) {
                GstState old_state, new_state;
                gst_message_parse_state_changed(message, &old_state, &new_state, NULL);
                if (new_state == GST_STATE_PLAYING) {
                    printf("Camera %d: Pipeline is now PLAYING\n", cam->camera_id);
                }
            }
            break;
        default:
            break;
    }
    return TRUE;
}

// Initialize GStreamer pipeline
int camera_gst_init(CameraInstance *cam) {
    GstCaps *caps, *sink_caps;
    GstBus *bus;
    GstAppSinkCallbacks callbacks = {NULL};
    char pipeline_str[MAX_PIPELINE_LEN];
    char appsrc_name[32], appsink_name[32];

    snprintf(appsrc_name, sizeof(appsrc_name), "appsrc_%d", cam->camera_id);
    snprintf(appsink_name, sizeof(appsink_name), "appsink_%d", cam->camera_id);

    snprintf(pipeline_str, sizeof(pipeline_str),
             "appsrc name=%s is-live=true do-timestamp=true format=time ! "
             "queue max-size-buffers=0 max-size-time=0 max-size-bytes=0 ! "
             "h264parse ! "
             "queue max-size-buffers=0 max-size-time=0 max-size-bytes=0 ! "
             "avdec_h264 ! "
             "videoconvert ! "
             "jpegenc quality=90 ! "
             "appsink name=%s emit-signals=true sync=false max-buffers=2 drop=true",
             appsrc_name, appsink_name);

    pthread_mutex_init(&cam->capture_mutex, NULL);
    cam->latest_sample = NULL;
    cam->frames_received = 0;
    cam->samples_received = 0;
    cam->timer = g_timer_new();

    GError *error = NULL;
    cam->pipeline = gst_parse_launch(pipeline_str, &error);
    if (!cam->pipeline) {
        g_printerr("Camera %d: Failed to create pipeline: %s\n", 
                   cam->camera_id, error ? error->message : "Unknown error");
        if (error) g_error_free(error);
        return FALSE;
    }

    cam->appsrc = gst_bin_get_by_name(GST_BIN(cam->pipeline), appsrc_name);
    cam->appsink = gst_bin_get_by_name(GST_BIN(cam->pipeline), appsink_name);
    if (!cam->appsrc || !cam->appsink) {
        g_printerr("Camera %d: Failed to get elements\n", cam->camera_id);
        return FALSE;
    }

    caps = gst_caps_new_simple("video/x-h264",
        "stream-format", G_TYPE_STRING, "byte-stream",
        "alignment", G_TYPE_STRING, "au",
        "width", G_TYPE_INT, 1920,
        "height", G_TYPE_INT, 960,
        "framerate", GST_TYPE_FRACTION, 29, 1,
        NULL);
    gst_app_src_set_caps(GST_APP_SRC(cam->appsrc), caps);
    gst_caps_unref(caps);

    g_object_set(G_OBJECT(cam->appsrc),
                 "stream-type", 0,
                 "is-live", TRUE,
                 "format", GST_FORMAT_TIME,
                 NULL);

    sink_caps = gst_caps_new_simple("image/jpeg", NULL);
    gst_app_sink_set_caps(GST_APP_SINK(cam->appsink), sink_caps);
    gst_caps_unref(sink_caps);

    g_object_set(cam->appsink, 
                 "max-buffers", 2,
                 "drop", TRUE,
                 "sync", FALSE,
                 NULL);

    callbacks.new_sample = on_new_sample;
    gst_app_sink_set_callbacks(GST_APP_SINK(cam->appsink), &callbacks, cam, NULL);

    bus = gst_pipeline_get_bus(GST_PIPELINE(cam->pipeline));
    cam->bus_watch_id = gst_bus_add_watch(bus, gst_bus_cb, cam);
    gst_object_unref(bus);

    printf("Camera %d: GStreamer pipeline initialized\n", cam->camera_id);
    return TRUE;
}

// UVC Callback
void cb(uvc_frame_t *frame, void *ptr) {
    CameraInstance *cam = (CameraInstance *)ptr;
    GstBuffer *buffer;
    GstFlowReturn ret;
    GstMapInfo map;
    static int first_frame[MAX_CAMERAS] = {1, 1};

    if (!cam->is_active || g_ctx.should_stop) return;
    if (frame->data_bytes == 0) return;

    cam->frames_received++;

    if (first_frame[cam->camera_id]) {
        printf("Camera %d: First frame received - %zu bytes\n", 
               cam->camera_id, frame->data_bytes);
        unsigned char *data = (unsigned char *)frame->data;
        if (frame->data_bytes >= 4) {
            printf("Camera %d: NAL header: %02X %02X %02X %02X\n",
                   cam->camera_id, data[0], data[1], data[2], data[3]);
        }
        first_frame[cam->camera_id] = 0;
    }

    buffer = gst_buffer_new_allocate(NULL, frame->data_bytes, NULL);
    if (!buffer) {
        fprintf(stderr, "Camera %d: Failed to allocate buffer\n", cam->camera_id);
        return;
    }

    GstClockTime ts = GST_CLOCK_TIME_NONE;
    if (cam->dwFrameInterval > 0) {
        ts = frame->sequence * (GstClockTime)cam->dwFrameInterval * 100;
    }
    GST_BUFFER_PTS(buffer) = ts;
    GST_BUFFER_DTS(buffer) = ts;
    GST_BUFFER_DURATION(buffer) = (GstClockTime)cam->dwFrameInterval * 100;

    if (gst_buffer_map(buffer, &map, GST_MAP_WRITE)) {
        memcpy(map.data, frame->data, frame->data_bytes);
        gst_buffer_unmap(buffer, &map);
    } else {
        gst_buffer_unref(buffer);
        return;
    }

    g_signal_emit_by_name(cam->appsrc, "push-buffer", buffer, &ret);
    gst_buffer_unref(buffer);

    if (ret != GST_FLOW_OK && ret != GST_FLOW_FLUSHING) {
        if (cam->framecount % 100 == 0) {
            fprintf(stderr, "Camera %d: Push buffer returned %d\n", cam->camera_id, ret);
        }
    }

    cam->framecount++;
    if (cam->framecount % 150 == 0) {
        g_print("Camera %d: Frames: %u, Decoded: %u, Saved: %u\n", 
                cam->camera_id, cam->framecount, 
                cam->samples_received, cam->saved_count);
    }
}

// Monitor thread
void *monitor_thread(void *arg) {
    while (!g_ctx.should_stop) {
        sleep(5);
        for (int i = 0; i < g_ctx.active_cameras; i++) {
            CameraInstance *cam = &g_ctx.cameras[i];
            if (cam->is_active && cam->is_streaming) {
                if (cam->framecount == cam->last_frame_count) {
                    printf("Camera %d: WARNING - No new frames in last 5 seconds\n", i);
                }
                cam->last_frame_count = cam->framecount;
            }
        }
    }
    return NULL;
}

// Stream thread
void *camera_stream_thread(void *arg) {
    CameraInstance *cam = (CameraInstance *)arg;
    uvc_error_t res;

    printf("Camera %d: Stream thread started\n", cam->camera_id);

    // Stagger start to reduce USB contention
    usleep(cam->camera_id * 3000000);  // 3 seconds per camera

    printf("Camera %d: Starting UVC streaming...\n", cam->camera_id);
    res = uvc_start_streaming(cam->devh, &cam->ctrl, cb, cam, 0);
    if (res < 0) {
        fprintf(stderr, "Camera %d: Failed to start streaming\n", cam->camera_id);
        uvc_perror(res, "uvc_start_streaming");
        cam->is_streaming = FALSE;
        cam->is_active = FALSE;
        return NULL;
    }

    cam->is_streaming = TRUE;
    printf("Camera %d: UVC streaming started successfully\n", cam->camera_id);

    while (cam->is_active && !g_ctx.should_stop) {
        usleep(100000);
    }

    printf("Camera %d: Stopping UVC streaming...\n", cam->camera_id);
    uvc_stop_streaming(cam->devh);
    cam->is_streaming = FALSE;
    printf("Camera %d: Stream thread ended\n", cam->camera_id);
    return NULL;
}

// Find and list THETA devices using shared context
int find_theta_devices() {
    uvc_device_t **device_list;
    uvc_error_t res;
    int found = 0;

    res = uvc_get_device_list(g_ctx.uvc_ctx, &device_list);
    if (res != UVC_SUCCESS) {
        fprintf(stderr, "Failed to get device list\n");
        return 0;
    }

    printf("Scanning for RICOH THETA X cameras (0x05ca:0x2717)...\n");

    for (int i = 0; device_list[i] != NULL && found < MAX_CAMERAS; i++) {
        uvc_device_descriptor_t *desc;
        if (uvc_get_device_descriptor(device_list[i], &desc) == UVC_SUCCESS) {
            if (desc->idVendor == 0x05ca && desc->idProduct == 0x2717) {
                g_ctx.cameras[found].dev = device_list[i];
                g_ctx.cameras[found].camera_id = found;
                if (desc->serialNumber) {
                    strncpy(g_ctx.cameras[found].serial, desc->serialNumber, 63);
                    g_ctx.cameras[found].serial[63] = '\0';
                } else {
                    snprintf(g_ctx.cameras[found].serial, 64, "THETA_UNKNOWN_%d", found);
                }
                uvc_ref_device(device_list[i]);  // Hold reference
                printf("Found THETA X camera %d: Serial='%s'\n", found, g_ctx.cameras[found].serial);
                found++;
            }
            uvc_free_device_descriptor(desc);
        }
    }

    uvc_free_device_list(device_list, 0);  // Don't auto-unref — we use uvc_ref_device
    return found;
}

// Initialize camera
int init_camera(CameraInstance *cam) {
    uvc_error_t res;
    char csv_filename[256];
    char timestamp[64];

    cam->is_active = FALSE;
    cam->is_streaming = FALSE;

    get_timestamp_string(timestamp, sizeof(timestamp));
    snprintf(csv_filename, sizeof(csv_filename), "captures/cam%d_log_%s.csv", 
             cam->camera_id, timestamp);
    cam->csv_file = fopen(csv_filename, "w");
    if (!cam->csv_file) {
        fprintf(stderr, "Camera %d: Failed to create CSV log file '%s': %s\n", 
                cam->camera_id, csv_filename, strerror(errno));
        return -1;
    }
    fprintf(cam->csv_file, "filename,timestamp_seconds\n");
    printf("Camera %d: Created CSV log\n", cam->camera_id);

    res = uvc_open(cam->dev, &cam->devh);
    if (res < 0) {
        fprintf(stderr, "Camera %d: Failed to open device\n", cam->camera_id);
        uvc_perror(res, "uvc_open");
        fclose(cam->csv_file);
        return -1;
    }
    printf("Camera %d: Device opened\n", cam->camera_id);

    res = uvc_get_stream_ctrl_format_size(
        cam->devh, &cam->ctrl,
        UVC_FRAME_FORMAT_H264,
        1920, 960,
        29
    );
    if (res < 0) {
        fprintf(stderr, "Camera %d: Failed to set format\n", cam->camera_id);
        uvc_perror(res, "uvc_get_stream_ctrl_format_size");
        uvc_close(cam->devh);
        fclose(cam->csv_file);
        return -1;
    }
    printf("Camera %d: Format set - 1920x960 @ 29 fps\n", cam->camera_id);
    printf("Camera %d: Frame interval: %u\n", cam->camera_id, cam->ctrl.dwFrameInterval);

    if (!camera_gst_init(cam)) {
        fprintf(stderr, "Camera %d: Failed to init GStreamer\n", cam->camera_id);
        uvc_close(cam->devh);
        fclose(cam->csv_file);
        return -1;
    }

    cam->dwFrameInterval = cam->ctrl.dwFrameInterval;
    cam->framecount = 0;
    cam->saved_count = 0;
    cam->last_frame_count = 0;

    GstStateChangeReturn state_ret = gst_element_set_state(cam->pipeline, GST_STATE_PLAYING);
    if (state_ret == GST_STATE_CHANGE_FAILURE) {
        fprintf(stderr, "Camera %d: Failed to start pipeline\n", cam->camera_id);
        gst_element_set_state(cam->pipeline, GST_STATE_NULL);
        uvc_close(cam->devh);
        fclose(cam->csv_file);
        return -1;
    }
    printf("Camera %d: Pipeline started\n", cam->camera_id);

    cam->timer_id = g_timeout_add(200, capture_timer_callback, cam);
    printf("Camera %d: Capture timer started (5Hz)\n", cam->camera_id);

    cam->is_active = TRUE;
    return 0;
}

// Cleanup camera
void cleanup_camera(CameraInstance *cam) {
    printf("Camera %d: Cleaning up...\n", cam->camera_id);
    cam->is_active = FALSE;
    if (cam->stream_thread) {
        pthread_join(cam->stream_thread, NULL);
    }
    if (cam->timer_id > 0) {
        g_source_remove(cam->timer_id);
    }
    if (cam->pipeline) {
        gst_element_set_state(cam->pipeline, GST_STATE_NULL);
        if (cam->bus_watch_id > 0) {
            g_source_remove(cam->bus_watch_id);
        }
        if (cam->timer) {
            g_timer_destroy(cam->timer);
        }
        pthread_mutex_destroy(&cam->capture_mutex);
        if (cam->latest_sample) {
            gst_sample_unref(cam->latest_sample);
        }
        gst_object_unref(cam->pipeline);
    }
    if (cam->devh) {
        uvc_close(cam->devh);
        printf("Camera %d: Device closed\n", cam->camera_id);
    }
    if (cam->csv_file) {
        fclose(cam->csv_file);
        printf("Camera %d: Saved %u images\n", cam->camera_id, cam->saved_count);
    }
}

// Keypress thread
void *keywait(void *arg) {
    getchar();
    g_ctx.should_stop = 1;
    g_main_loop_quit(g_ctx.loop);
    return NULL;
}

// Main
int main(int argc, char **argv) {
    memset(&g_ctx, 0, sizeof(g_ctx));
    create_output_dir("captures");
    gst_init(&argc, &argv);

    // Initialize SINGLE shared UVC context
    uvc_error_t res = uvc_init(&g_ctx.uvc_ctx, NULL);
    if (res < 0) {
        uvc_perror(res, "uvc_init");
        return -1;
    }

    g_ctx.active_cameras = find_theta_devices();
    if (g_ctx.active_cameras == 0) {
        fprintf(stderr, "No RICOH THETA X cameras found\n");
        uvc_exit(g_ctx.uvc_ctx);
        return -1;
    }

    printf("Found %d THETA X camera(s)\n", g_ctx.active_cameras);

    int initialized = 0;
    for (int i = 0; i < g_ctx.active_cameras; i++) {
        CameraInstance *cam = &g_ctx.cameras[i];
        printf("Initializing camera %d (Serial: %s)...\n", i, cam->serial);
        if (init_camera(cam) == 0) {
            initialized++;
            printf("Camera %d initialized successfully\n", i);
        } else {
            fprintf(stderr, "Failed to initialize camera %d\n", i);
        }
    }

    if (initialized == 0) {
        fprintf(stderr, "No cameras initialized\n");
        goto cleanup;
    }

    printf("%d camera(s) initialized\n", initialized);

    g_ctx.loop = g_main_loop_new(NULL, FALSE);

    printf("Starting streaming threads...\n");
    for (int i = 0; i < g_ctx.active_cameras; i++) {
        if (g_ctx.cameras[i].is_active) {
            printf("Starting stream thread for camera %d\n", i);
            pthread_create(&g_ctx.cameras[i].stream_thread, NULL, 
                          camera_stream_thread, &g_ctx.cameras[i]);
        }
    }

    pthread_create(&g_ctx.monitor_thread, NULL, monitor_thread, NULL);

    printf("\nCapturing at %d Hz from %d camera(s)\n", CAPTURE_RATE_HZ, initialized);
    printf("Press ENTER to stop...\n");
    pthread_create(&g_ctx.key_thread, NULL, keywait, NULL);

    g_main_loop_run(g_ctx.loop);
    pthread_join(&g_ctx.key_thread, NULL);

cleanup:
    printf("\nStopping capture...\n");
    g_ctx.should_stop = 1;
    if (g_ctx.monitor_thread) {
        pthread_join(g_ctx.monitor_thread, NULL);
    }
    for (int i = 0; i < g_ctx.active_cameras; i++) {
        if (g_ctx.cameras[i].is_active) {
            cleanup_camera(&g_ctx.cameras[i]);
        }
    }
    if (g_ctx.uvc_ctx) {
        uvc_exit(g_ctx.uvc_ctx);
    }
    if (g_ctx.loop) {
        g_main_loop_unref(g_ctx.loop);
    }
    printf("Done.\n");
    return 0;
}