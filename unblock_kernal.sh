#!/bin/bash

echo "=== Resetting RICOH THETA X cameras ==="

VENDOR="05ca"
PRODUCT="2717"

# Step 1: Register with uvcvideo driver (for proper unbinding)
echo "Registering THETA X with uvcvideo driver..."
echo "$VENDOR $PRODUCT" | sudo tee /sys/bus/usb/drivers/uvcvideo/new_id > /dev/null 2>&1 || true

# Step 2: Find and unbind ALL interfaces for THETA X devices
for devpath in /sys/bus/usb/devices/*; do
    if [ -f "$devpath/idVendor" ] && [ -f "$devpath/idProduct" ]; then
        vid=$(cat "$devpath/idVendor" 2>/dev/null | tr -d '\n')
        pid=$(cat "$devpath/idProduct" 2>/dev/null | tr -d '\n')

        if [ "$vid" = "$VENDOR" ] && [ "$pid" = "$PRODUCT" ]; then
            echo "Found THETA X at $devpath"

            # Unbind all interfaces (more thorough)
            for intf in "$devpath"/*:*.*; do
                if [ -d "$intf" ]; then
                    intf_name=$(basename "$intf")
                    echo "  Checking interface $intf_name"
                    
                    # Force unbind if driver is bound
                    if [ -f "$intf/driver/unbind" ]; then
                        echo "    Unbinding interface $intf_name"
                        echo "$intf_name" | sudo tee "$intf/driver/unbind" > /dev/null 2>&1 || true
                    fi
                fi
            done
            
            # Force re-enumeration
            echo "  Resetting device to force kernel to release it"
            echo 0 | sudo tee "$devpath/authorized" > /dev/null 2>&1 || true
            sleep 1
            echo 1 | sudo tee "$devpath/authorized" > /dev/null 2>&1 || true
        fi
    fi
done

echo "Waiting 5 seconds for stabilization..."
sleep 5

# Verify no kernel driver is claiming the devices
echo "Verifying devices are not claimed by kernel..."
for devpath in /sys/bus/usb/devices/*; do
    if [ -f "$devpath/idVendor" ] && [ -f "$devpath/idProduct" ]; then
        vid=$(cat "$devpath/idVendor" 2>/dev/null | tr -d '\n')
        pid=$(cat "$devpath/idProduct" 2>/dev/null | tr -d '\n')
        if [ "$vid" = "$VENDOR" ] && [ "$pid" = "$PRODUCT" ]; then
            echo "Device at $devpath:"
            for intf in "$devpath"/*:*.*; do
                if [ -d "$intf" ] && [ -f "$intf/driver" ]; then
                    driver=$(basename $(readlink -f "$intf/driver"))
                    echo "  Interface $(basename $intf): Driver=$driver"
                fi
            done
        fi
    fi
done

echo "=== Done. Run your program with sudo. ==="