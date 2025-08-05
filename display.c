#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>

#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <libuvc/libuvc.h>

#define MAX_PIPELINE_LEN 1024

// Global GStreamer source context
struct gst_src {
    GstElement *pipeline;
    GstElement *appsrc;
    GMainLoop *loop;
    GTimer *timer;
    guint framecount;
    guint bus_watch_id;
    uint32_t dwFrameInterval;
} src;

// GStreamer bus callback
static gboolean
gst_bus_cb(GstBus *bus, GstMessage *message, gpointer data)
{
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

// Initialize GStreamer pipeline
int gst_src_init(int *argc, char ***argv, char *sink_pipeline)
{
    GstCaps *caps;
    GstBus *bus;
    char pipeline_str[MAX_PIPELINE_LEN];

    // Use decodebin for robust H.264 handling
    snprintf(pipeline_str, sizeof(pipeline_str),
             "appsrc name=ap is-live=true do-timestamp=true ! "
             "queue ! h264parse ! queue ! %s", sink_pipeline);

    gst_init(argc, argv);

    src.timer = g_timer_new();
    src.loop = g_main_loop_new(NULL, FALSE);
    src.pipeline = gst_parse_launch(pipeline_str, NULL);

    if (!src.pipeline) {
        g_printerr("Failed to create GStreamer pipeline\n");
        return FALSE;
    }

    src.appsrc = gst_bin_get_by_name(GST_BIN(src.pipeline), "ap");
    if (!src.appsrc) {
        g_printerr("Failed to get 'appsrc' element\n");
        return FALSE;
    }

    // Add profile for better H.264 handling
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

    bus = gst_pipeline_get_bus(GST_PIPELINE(src.pipeline));
    src.bus_watch_id = gst_bus_add_watch(bus, gst_bus_cb, NULL);
    gst_object_unref(bus);

    return TRUE;
}

// Thread to stop on keypress
void *keywait(void *arg)
{
    getchar();  // Wait for Enter or any key
    g_main_loop_quit(src.loop);
    return NULL;
}

// UVC Callback: Push H.264 frame into GStreamer
void cb(uvc_frame_t *frame, void *ptr)
{
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
    if (src.framecount % 30 == 0) {
        g_print("Streamed %u frames\n", src.framecount);
    }
}

// Find ANY UVC device
uvc_error_t find_uvc_device(uvc_context_t *ctx, uvc_device_t **dev_out)
{
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
uvc_error_t set_h264_format(uvc_device_handle_t *devh, uvc_stream_ctrl_t *ctrl)
{
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

// Fixed: Use correct field names from uvc_stream_ctrl_t
void print_ctrl(uvc_stream_ctrl_t *ctrl)
{
    printf("Stream Config:\n");
    printf("  Format: H.264\n");
    // Use .wWidth and .wHeight (older libuvc) OR .width and .height (newer)
    // Since you got error, let's use direct access:
    printf("  Frame Interval: %u (1/%.2f fps)\n",
           ctrl->dwFrameInterval, 10000000.0f / ctrl->dwFrameInterval);
    printf("  Max Frame Size: %u\n", ctrl->dwMaxVideoFrameSize);
    printf("  Max Payload: %u\n", ctrl->dwMaxPayloadTransferSize);
}

// Main
int main(int argc, char **argv)
{
    uvc_context_t *ctx = NULL;
    uvc_device_t *dev = NULL;
    uvc_device_handle_t *devh = NULL;
    uvc_stream_ctrl_t ctrl;
    uvc_error_t res;
    char *sink_pipeline = NULL;

    // Determine sink based on program name
    const char *cmd = strrchr(argv[0], '/');
    cmd = cmd ? cmd + 1 : argv[0];

    if (strcmp(cmd, "gst_loopback") == 0) {
        sink_pipeline = "decodebin ! videoconvert ! v4l2sink device=/dev/video1 sync=false";
    } else {
        sink_pipeline = "decodebin ! videoconvert ! autovideosink sync=false";
    }

    // Initialize UVC
    res = uvc_init(&ctx, NULL);
    if (res < 0) {
        uvc_perror(res, "uvc_init");
        return -1;
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
    if (!gst_src_init(&argc, &argv, sink_pipeline)) {
        fprintf(stderr, "Failed to initialize GStreamer\n");
        goto close_device;
    }

    // Set timing
    src.dwFrameInterval = ctrl.dwFrameInterval;
    src.framecount = 0;

    // Start GStreamer
    gst_element_set_state(src.pipeline, GST_STATE_PLAYING);
    printf("GStreamer pipeline started\n");

    // Start UVC streaming
    res = uvc_start_streaming(devh, &ctrl, cb, NULL, 0);
    if (res < 0) {
        uvc_perror(res, "uvc_start_streaming");
        goto stop_pipeline;
    }

    printf("Streaming... Press ENTER to stop.\n");

    // Start keypress thread
    pthread_t key_thread;
    pthread_create(&key_thread, NULL, keywait, NULL);

    // Run GStreamer loop
    g_main_loop_run(src.loop);

    // Stop
    uvc_stop_streaming(devh);
    printf("Streaming stopped.\n");

    // Join thread
    pthread_join(key_thread, NULL);

stop_pipeline:
    gst_element_set_state(src.pipeline, GST_STATE_NULL);
    g_source_remove(src.bus_watch_id);
    g_main_loop_unref(src.loop);
    g_timer_destroy(src.timer);

close_device:
    uvc_close(devh);
    uvc_unref_device(dev);
    printf("Device closed.\n");

cleanup:
    uvc_exit(ctx);
    printf("UVC exited.\n");

    return res;
}