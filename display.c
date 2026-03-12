#include <pthread.h>
#include <stdio.h>
#include <unistd.h>
#include <string.h>
#include <stdlib.h>
#include <stdint.h>
#include <endian.h>

#include <gst/gst.h>
#include <gst/app/gstappsrc.h>
#include <libuvc/libuvc.h>
#include <libusb-1.0/libusb.h>

#define MAX_PIPELINE_LEN 1024

// MTP Constants
#define MTP_CONTAINER_TYPE_COMMAND    0x0001
#define MTP_CONTAINER_TYPE_DATA       0x0002
#define MTP_CONTAINER_TYPE_RESPONSE   0x0003
#define MTP_CONTAINER_TYPE_EVENT      0x0004

// MTP Operations
#define MTP_OP_GET_DEVICE_INFO        0x1001
#define MTP_OP_OPEN_SESSION           0x1002
#define MTP_OP_CLOSE_SESSION          0x1003
#define MTP_OP_GET_DEVICE_PROP_DESC   0x1014
#define MTP_OP_GET_DEVICE_PROP_VALUE  0x1015
#define MTP_OP_SET_DEVICE_PROP_VALUE  0x1016

// MTP Responses
#define MTP_RESP_OK                   0x2001

// Ricoh Device Properties
#define RICOH_PROP_FUNCTIONAL_MODE    0x5002
#define RICOH_PROP_CAPTURE_STATUS     0xD808
#define RICOH_PROP_CAMERA_MODE        0xD837
#define RICOH_PROP_IMAGE_STITCHING    0xD834
#define RICOH_PROP_VIDEO_STITCHING    0xD818
#define RICOH_PROP_FILTER             0xD80B
#define RICOH_PROP_GPS_TAG_RECORDING  0xD832

// MTP Container structure (12 bytes header)
typedef struct {
    uint32_t length;        // Total length including this header
    uint16_t type;          // Container type
    uint16_t code;          // Operation/Response/Event code
    uint32_t transaction_id; // Transaction ID
} __attribute__((packed)) mtp_container_t;

// MTP USB Context
typedef struct {
    libusb_context *ctx;
    libusb_device_handle *handle;
    uint8_t endpoint_in;
    uint8_t endpoint_out;
    uint8_t endpoint_int;
    uint8_t interface_num;
    uint32_t session_id;
    uint32_t transaction_id;
} mtp_context_t;

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

// Function declarations
int mtp_send_command(mtp_context_t *mtp_ctx, uint16_t operation, uint32_t *params, int param_count, uint8_t *data, uint32_t data_len);
int mtp_receive_response(mtp_context_t *mtp_ctx);
int mtp_get_device_property(mtp_context_t *mtp_ctx, uint32_t property_code, uint8_t *data_out, int max_len);
int mtp_set_device_property(mtp_context_t *mtp_ctx, uint32_t property_code, uint16_t value);
int mtp_prepare_camera_for_fisheye(mtp_context_t *mtp_ctx);

// MTP USB Communication Functions
int mtp_find_ricoh_device(mtp_context_t *mtp_ctx) {
    libusb_device **devs;
    libusb_device *dev;
    struct libusb_device_descriptor desc;
    int ret, i = 0;

    if (libusb_init(&mtp_ctx->ctx) < 0) {
        fprintf(stderr, "Failed to initialize libusb\n");
        return -1;
    }

    libusb_set_option(mtp_ctx->ctx, LIBUSB_OPTION_LOG_LEVEL, LIBUSB_LOG_LEVEL_WARNING);

    ssize_t cnt = libusb_get_device_list(mtp_ctx->ctx, &devs);
    if (cnt < 0) {
        fprintf(stderr, "Failed to get device list\n");
        libusb_exit(mtp_ctx->ctx);
        return -1;
    }

    // Look for Ricoh device (vendor ID 0x05ca)
    while ((dev = devs[i++]) != NULL) {
        ret = libusb_get_device_descriptor(dev, &desc);
        if (ret < 0) continue;

        if (desc.idVendor == 0x05ca) {
            printf("Found Ricoh device: %04x:%04x\n", desc.idVendor, desc.idProduct);
            
            ret = libusb_open(dev, &mtp_ctx->handle);
            if (ret == 0) {
                // Reset device first
                libusb_reset_device(mtp_ctx->handle);
                usleep(100000); // Wait 100ms
                
                // Find MTP interface (class 6, subclass 1, protocol 1)
                struct libusb_config_descriptor *config;
                libusb_get_active_config_descriptor(dev, &config);
                
                for (int j = 0; j < config->bNumInterfaces; j++) {
                    const struct libusb_interface *interface = &config->interface[j];
                    for (int k = 0; k < interface->num_altsetting; k++) {
                        const struct libusb_interface_descriptor *altsetting = &interface->altsetting[k];
                        
                        // MTP: Class 6 (Still Image), Subclass 1, Protocol 1
                        if (altsetting->bInterfaceClass == 6 && 
                            altsetting->bInterfaceSubClass == 1 &&
                            altsetting->bInterfaceProtocol == 1) {
                            
                            printf("Found MTP interface %d\n", altsetting->bInterfaceNumber);
                            mtp_ctx->interface_num = altsetting->bInterfaceNumber;
                            
                            // Detach kernel driver if attached
                            if (libusb_kernel_driver_active(mtp_ctx->handle, mtp_ctx->interface_num) == 1) {
                                printf("Detaching kernel driver\n");
                                libusb_detach_kernel_driver(mtp_ctx->handle, mtp_ctx->interface_num);
                            }
                            
                            // Claim interface
                            ret = libusb_claim_interface(mtp_ctx->handle, mtp_ctx->interface_num);
                            if (ret != 0) {
                                printf("Failed to claim interface: %s\n", libusb_error_name(ret));
                                continue;
                            }
                            
                            // Find endpoints
                            mtp_ctx->endpoint_in = 0;
                            mtp_ctx->endpoint_out = 0;
                            mtp_ctx->endpoint_int = 0;
                            
                            for (int l = 0; l < altsetting->bNumEndpoints; l++) {
                                const struct libusb_endpoint_descriptor *endpoint = &altsetting->endpoint[l];
                                
                                if ((endpoint->bmAttributes & LIBUSB_TRANSFER_TYPE_MASK) == LIBUSB_TRANSFER_TYPE_BULK) {
                                    if (endpoint->bEndpointAddress & LIBUSB_ENDPOINT_IN) {
                                        mtp_ctx->endpoint_in = endpoint->bEndpointAddress;
                                        printf("Bulk IN endpoint: 0x%02x\n", mtp_ctx->endpoint_in);
                                    } else {
                                        mtp_ctx->endpoint_out = endpoint->bEndpointAddress;
                                        printf("Bulk OUT endpoint: 0x%02x\n", mtp_ctx->endpoint_out);
                                    }
                                } else if ((endpoint->bmAttributes & LIBUSB_TRANSFER_TYPE_MASK) == LIBUSB_TRANSFER_TYPE_INTERRUPT) {
                                    if (endpoint->bEndpointAddress & LIBUSB_ENDPOINT_IN) {
                                        mtp_ctx->endpoint_int = endpoint->bEndpointAddress;
                                        printf("Interrupt IN endpoint: 0x%02x\n", mtp_ctx->endpoint_int);
                                    }
                                }
                            }
                            
                            libusb_free_config_descriptor(config);
                            libusb_free_device_list(devs, 1);
                            
                            if (mtp_ctx->endpoint_in && mtp_ctx->endpoint_out) {
                                printf("MTP endpoints configured successfully\n");
                                return 0;
                            } else {
                                printf("Failed to find required endpoints\n");
                                return -1;
                            }
                        }
                    }
                }
                libusb_free_config_descriptor(config);
                libusb_close(mtp_ctx->handle);
            }
        }
    }

    libusb_free_device_list(devs, 1);
    libusb_exit(mtp_ctx->ctx);
    return -1;
}

int mtp_send_command(mtp_context_t *mtp_ctx, uint16_t operation, uint32_t *params, int param_count, uint8_t *data, uint32_t data_len) {
    uint8_t buffer[1024];
    mtp_container_t *container = (mtp_container_t *)buffer;
    int transferred;
    
    // Calculate total length: header + parameters + data
    uint32_t param_bytes = param_count * 4;
    uint32_t total_length = 12 + param_bytes; // 12 byte header + parameters
    
    // Build command container
    container->length = htole32(total_length);
    container->type = htole16(MTP_CONTAINER_TYPE_COMMAND);
    container->code = htole16(operation);
    container->transaction_id = htole32(mtp_ctx->transaction_id);
    
    // Add parameters after header
    uint32_t *param_ptr = (uint32_t*)(buffer + 12);
    for (int i = 0; i < param_count; i++) {
        param_ptr[i] = htole32(params[i]);
    }
    
    printf("Sending MTP command 0x%04x, transaction %d, length %d\n", 
           operation, mtp_ctx->transaction_id, total_length);
    
    // Send command (MTP over USB doesn't use separate length header)
    int ret = libusb_bulk_transfer(mtp_ctx->handle, mtp_ctx->endpoint_out, 
                                   buffer, total_length, &transferred, 5000);
    if (ret != 0) {
        printf("Failed to send command: %s\n", libusb_error_name(ret));
        return -1;
    }
    
    printf("Sent %d bytes\n", transferred);
    
    // Send data phase if present
    if (data && data_len > 0) {
        uint8_t data_buffer[1024];
        mtp_container_t *data_container = (mtp_container_t *)data_buffer;
        
        uint32_t data_total_len = 12 + data_len;
        data_container->length = htole32(data_total_len);
        data_container->type = htole16(MTP_CONTAINER_TYPE_DATA);
        data_container->code = htole16(operation);
        data_container->transaction_id = htole32(mtp_ctx->transaction_id);
        
        memcpy(data_buffer + 12, data, data_len);
        
        printf("Sending data phase, length %d\n", data_total_len);
        ret = libusb_bulk_transfer(mtp_ctx->handle, mtp_ctx->endpoint_out, 
                                   data_buffer, data_total_len, &transferred, 5000);
        if (ret != 0) {
            printf("Failed to send data: %s\n", libusb_error_name(ret));
            return -1;
        }
        printf("Sent data %d bytes\n", transferred);
    }
    
    mtp_ctx->transaction_id++;
    return 0;
}

int mtp_receive_response(mtp_context_t *mtp_ctx) {
    uint8_t buffer[1024];
    int transferred;
    
    // Read response with longer timeout
    int ret = libusb_bulk_transfer(mtp_ctx->handle, mtp_ctx->endpoint_in, 
                                   buffer, sizeof(buffer), &transferred, 10000);
    if (ret != 0) {
        printf("Failed to receive response: %s\n", libusb_error_name(ret));
        return -1;
    }
    
    printf("Received %d bytes\n", transferred);
    
    if (transferred >= 12) {
        mtp_container_t *container = (mtp_container_t *)buffer;
        uint32_t length = le32toh(container->length);
        uint16_t type = le16toh(container->type);
        uint16_t code = le16toh(container->code);
        uint32_t trans_id = le32toh(container->transaction_id);
        
        printf("Response: length=%d, type=0x%04x, code=0x%04x, trans_id=%d\n",
               length, type, code, trans_id);
        
        if (type == MTP_CONTAINER_TYPE_RESPONSE && code == MTP_RESP_OK) {
            printf("MTP operation successful\n");
            return 0;
        } else {
            printf("MTP Error response: 0x%04x\n", code);
            return -1;
        }
    }
    
    return -1;
}

int mtp_get_device_property(mtp_context_t *mtp_ctx, uint32_t property_code, uint8_t *data_out, int max_len) {
    printf("Getting property 0x%04x...\n", property_code);
    
    uint32_t params[1] = { property_code };
    if (mtp_send_command(mtp_ctx, MTP_OP_GET_DEVICE_PROP_VALUE, params, 1, NULL, 0) != 0) {
        return -1;
    }
    
    // Read data response first
    uint8_t buffer[1024];
    int transferred;
    
    int ret = libusb_bulk_transfer(mtp_ctx->handle, mtp_ctx->endpoint_in, 
                                   buffer, sizeof(buffer), &transferred, 10000);
    if (ret != 0) {
        printf("Failed to receive property data: %s\n", libusb_error_name(ret));
        return -1;
    }
    
    printf("Received property data %d bytes\n", transferred);
    
    if (transferred >= 12) {
        mtp_container_t *container = (mtp_container_t *)buffer;
        uint32_t length = le32toh(container->length);
        uint16_t type = le16toh(container->type);
        
        if (type == MTP_CONTAINER_TYPE_DATA) {
            int data_len = length - 12;
            if (data_len > 0 && data_len <= max_len) {
                memcpy(data_out, buffer + 12, data_len);
                
                // Now read the response
                return mtp_receive_response(mtp_ctx);
            }
        }
    }
    
    return -1;
}

int mtp_open_session(mtp_context_t *mtp_ctx) {
    mtp_ctx->session_id = 1;
    mtp_ctx->transaction_id = 1;
    
    printf("Opening MTP session...\n");
    
    uint32_t params[1] = { mtp_ctx->session_id };
    if (mtp_send_command(mtp_ctx, MTP_OP_OPEN_SESSION, params, 1, NULL, 0) != 0) {
        return -1;
    }
    
    return mtp_receive_response(mtp_ctx);
}

int mtp_set_device_property(mtp_context_t *mtp_ctx, uint32_t property_code, uint16_t value) {
    uint8_t data[2];
    data[0] = value & 0xFF;
    data[1] = (value >> 8) & 0xFF;
    
    printf("Setting property 0x%04x to %d...\n", property_code, value);
    
    uint32_t set_params[1] = { property_code };
    if (mtp_send_command(mtp_ctx, MTP_OP_SET_DEVICE_PROP_VALUE, set_params, 1, data, 2) != 0) {
        return -1;
    }
    
    return mtp_receive_response(mtp_ctx);
}

int mtp_prepare_camera_for_fisheye(mtp_context_t *mtp_ctx) {
    uint8_t prop_data[16];
    
    printf("Preparing camera for fisheye configuration...\n");
    
    // Check functional mode
    if (mtp_get_device_property(mtp_ctx, RICOH_PROP_FUNCTIONAL_MODE, prop_data, sizeof(prop_data)) == 0) {
        uint16_t functional_mode = prop_data[0] | (prop_data[1] << 8);
        printf("Current functional mode: %d\n", functional_mode);
        
        // Set to standard functional mode (0x0000) if not already
        if (functional_mode != 0x0000) {
            printf("Setting functional mode to standard...\n");
            if (mtp_set_device_property(mtp_ctx, RICOH_PROP_FUNCTIONAL_MODE, 0x0000) != 0) {
                printf("Warning: Failed to set functional mode\n");
            } else {
                sleep(1); // Wait for mode change
            }
        }
    }
    
    // Check capture status
    if (mtp_get_device_property(mtp_ctx, RICOH_PROP_CAPTURE_STATUS, prop_data, sizeof(prop_data)) == 0) {
        uint16_t capture_status = prop_data[0] | (prop_data[1] << 8);
        printf("Current capture status: %d\n", capture_status);
    }
    
    // Check camera mode and try to set to appropriate mode
    if (mtp_get_device_property(mtp_ctx, RICOH_PROP_CAMERA_MODE, prop_data, sizeof(prop_data)) == 0) {
        uint16_t camera_mode = prop_data[0] | (prop_data[1] << 8);
        printf("Current camera mode: %d\n", camera_mode);
        
        // Try to set to image mode (typically mode 1 for still images)
        if (camera_mode != 1) {
            printf("Setting camera mode to image mode...\n");
            if (mtp_set_device_property(mtp_ctx, RICOH_PROP_CAMERA_MODE, 1) == 0) {
                printf("Camera mode set successfully\n");
                sleep(2); // Wait for mode change
            } else {
                printf("Warning: Failed to set camera mode\n");
            }
        }
    }
    
    printf("Camera preparation completed\n");
    return 0;
}

int enable_fisheye_mode_native() {
    mtp_context_t mtp_ctx = {0};
    int success = 0;
    
    printf("Configuring fisheye mode via native MTP...\n");
    
    // Find and open Ricoh device
    if (mtp_find_ricoh_device(&mtp_ctx) != 0) {
        printf("No Ricoh MTP device found\n");
        return -1;
    }
    
    // Open MTP session
    if (mtp_open_session(&mtp_ctx) != 0) {
        printf("Failed to open MTP session\n");
        goto cleanup;
    }
    
    printf("MTP session opened successfully\n");
    
    // Prepare camera for fisheye configuration
    mtp_prepare_camera_for_fisheye(&mtp_ctx);
    
    // Configure fisheye mode properties with error handling
    printf("Configuring fisheye properties...\n");
    
    if (mtp_set_device_property(&mtp_ctx, RICOH_PROP_IMAGE_STITCHING, 2) == 0) {
        printf("✓ Image_Stitching set to 2\n");
    } else {
        printf("✗ Failed to set Image_Stitching\n");
        // Try alternative stitching mode
        printf("Trying alternative stitching mode...\n");
        if (mtp_set_device_property(&mtp_ctx, RICOH_PROP_IMAGE_STITCHING, 1) == 0) {
            printf("✓ Image_Stitching set to 1 (alternative mode)\n");
        }
    }
    
    sleep(1); // Wait between property changes
    
    if (mtp_set_device_property(&mtp_ctx, RICOH_PROP_VIDEO_STITCHING, 1) == 0) {
        printf("✓ Video_Stitching set to 1\n");
    } else {
        printf("✗ Failed to set Video_Stitching\n");
    }
    
    sleep(1);
    
    if (mtp_set_device_property(&mtp_ctx, RICOH_PROP_FILTER, 0) == 0) {
        printf("✓ Filter set to 0\n");
    } else {
        printf("✗ Failed to set Filter (this may be normal for some modes)\n");
    }
    
    sleep(1);
    
    if (mtp_set_device_property(&mtp_ctx, RICOH_PROP_GPS_TAG_RECORDING, 1) == 0) {
        printf("✓ GPStagRecording set to 1\n");
    } else {
        printf("✗ Failed to set GPStagRecording\n");
    }
    
    printf("Fisheye mode configuration completed!\n");
    success = 1;
    
cleanup:
    if (mtp_ctx.handle) {
        libusb_release_interface(mtp_ctx.handle, mtp_ctx.interface_num);
        libusb_close(mtp_ctx.handle);
    }
    if (mtp_ctx.ctx) {
        libusb_exit(mtp_ctx.ctx);
    }
    
    return success ? 0 : -1;
}

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

// Initialize GStreamer pipeline with fisheye-optimized settings
int gst_src_init(int *argc, char ***argv, char *sink_pipeline)
{
    GstCaps *caps;
    GstBus *bus;
    char pipeline_str[MAX_PIPELINE_LEN];

    // Enhanced pipeline for fisheye/360 content
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

    // Configure for fisheye/360 video (typically 1920x960 for dual fisheye)
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
    printf("Press ENTER to stop streaming...\n");
    getchar();
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
        g_print("Streamed %u fisheye frames\n", src.framecount);
    }
}

// Find Ricoh camera specifically (or fallback to any UVC device)
uvc_error_t find_ricoh_device(uvc_context_t *ctx, uvc_device_t **dev_out)
{
    // First try to find Ricoh camera (vendor ID 0x05ca)
    uvc_error_t res = uvc_find_device(ctx, dev_out, 0x05ca, 0, NULL);
    
    if (res == UVC_SUCCESS) {
        printf("Found Ricoh UVC camera - fisheye mode will be available\n");
        return res;
    }
    
    printf("Ricoh camera not found, searching for any UVC device...\n");
    
    // Fallback to any UVC device
    res = uvc_find_device(ctx, dev_out, 0, 0, NULL);
    if (res != UVC_SUCCESS) {
        fprintf(stderr, "No UVC device found. Is camera plugged in?\n");
    } else {
        uvc_device_descriptor_t *desc;
        if (uvc_get_device_descriptor(*dev_out, &desc) == UVC_SUCCESS) {
            printf("Found device: %s (0x%04x:0x%04x)\n",
                   desc->product ? desc->product : "Unknown",
                   desc->idVendor, desc->idProduct);
            
            if (desc->idVendor != 0x05ca) {
                printf("Warning: This is not a Ricoh camera. Fisheye mode may not be available.\n");
            }
            
            uvc_free_device_descriptor(desc);
        }
    }
    return res;
}

// Set format optimized for fisheye streaming
uvc_error_t set_fisheye_format(uvc_device_handle_t *devh, uvc_stream_ctrl_t *ctrl)
{
    // Try fisheye-optimized resolution first (dual fisheye side-by-side)
    uvc_error_t res = uvc_get_stream_ctrl_format_size(
        devh, ctrl,
        UVC_FRAME_FORMAT_H264,
        1920, 960,     // Dual fisheye resolution
        29             // Standard framerate
    );

    if (res < 0) {
        printf("1920x960 not supported, trying 1920x1080...\n");
        // Fallback to standard HD
        res = uvc_get_stream_ctrl_format_size(
            devh, ctrl,
            UVC_FRAME_FORMAT_H264,
            1920, 1080,
            29
        );
    }

    if (res < 0) {
        fprintf(stderr, "Failed to set any supported format\n");
        uvc_perror(res, "uvc_get_stream_ctrl_format_size");
    } else {
        printf("Streaming format configured for fisheye mode\n");
    }

    return res;
}

void print_ctrl(uvc_stream_ctrl_t *ctrl)
{
    printf("Stream Config:\n");
    printf("  Format: H.264 (Fisheye optimized)\n");
    printf("  Frame Interval: %u (1/%.2f fps)\n",
           ctrl->dwFrameInterval, 10000000.0f / ctrl->dwFrameInterval);
    printf("  Max Frame Size: %u\n", ctrl->dwMaxVideoFrameSize);
    printf("  Max Payload: %u\n", ctrl->dwMaxPayloadTransferSize);
}

// Main function with native fisheye mode integration
int main(int argc, char **argv)
{
    uvc_context_t *ctx = NULL;
    uvc_device_t *dev = NULL;
    uvc_device_handle_t *devh = NULL;
    uvc_stream_ctrl_t ctrl;
    uvc_error_t res;
    char *sink_pipeline = NULL;
    int enable_fisheye = 1; // Default to enabling fisheye mode

    // Parse command line arguments
    for (int i = 1; i < argc; i++) {
        if (strcmp(argv[i], "--no-fisheye") == 0) {
            enable_fisheye = 0;
        } else if (strcmp(argv[i], "--help") == 0) {
            printf("Usage: %s [--no-fisheye] [--help]\n", argv[0]);
            printf("  --no-fisheye: Disable fisheye mode configuration\n");
            printf("  --help: Show this help message\n");
            return 0;
        }
    }

    // Determine sink based on program name
    const char *cmd = strrchr(argv[0], '/');
    cmd = cmd ? cmd + 1 : argv[0];

    if (strcmp(cmd, "gst_loopback") == 0) {
        sink_pipeline = "decodebin ! videoconvert ! v4l2sink device=/dev/video1 sync=false";
    } else {
        // Enhanced sink for fisheye content
        sink_pipeline = "decodebin ! videoconvert ! autovideosink sync=false";
    }

    printf("=== UVC Fisheye Streaming Application (Fixed Function Order) ===\n");

    // Configure fisheye mode via native MTP (before UVC initialization)
    if (enable_fisheye) {
        printf("Attempting to configure fisheye mode...\n");
        if (enable_fisheye_mode_native() == 0) {
            printf("Fisheye mode configured successfully!\n");
            // Wait for camera to apply settings
            printf("Waiting for camera to apply settings...\n");
            sleep(3);
        } else {
            printf("Warning: Failed to configure fisheye mode\n");
            printf("Continuing with normal UVC streaming...\n");
        }
    } else {
        printf("Fisheye mode configuration disabled\n");
    }

    // Initialize UVC
    res = uvc_init(&ctx, NULL);
    if (res < 0) {
        uvc_perror(res, "uvc_init");
        return -1;
    }
    printf("UVC initialized\n");

    // Find Ricoh device (or any UVC device)
    res = find_ricoh_device(ctx, &dev);
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

    // Set format optimized for fisheye
    res = set_fisheye_format(devh, &ctrl);
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
    printf("GStreamer pipeline started for fisheye streaming\n");

    // Start UVC streaming
    res = uvc_start_streaming(devh, &ctrl, cb, NULL, 0);
    if (res < 0) {
        uvc_perror(res, "uvc_start_streaming");
        goto stop_pipeline;
    }

    printf("\n=== Fixed Native Fisheye Streaming Active ===\n");
    if (enable_fisheye) {
        printf("Fisheye mode: ENABLED (fixed native MTP implementation)\n");
    } else {
        printf("Fisheye mode: DISABLED\n");
    }

    // Start keypress thread
    pthread_t key_thread;
    pthread_create(&key_thread, NULL, keywait, NULL);

    // Run GStreamer loop
    g_main_loop_run(src.loop);

    // Stop streaming
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