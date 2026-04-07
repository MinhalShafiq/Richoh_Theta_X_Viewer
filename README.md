# Richoh Theta X Viewer

A lightweight viewer application for the RICOH THETA X camera using USB Video Class (UVC) streaming.

## Purpose

This project demonstrates how to capture and display live video from a RICOH THETA X device over USB using the `libuvc-theta` library. It is intended for developers who want a simple example of RICOH THETA X integration on Linux.

## Prerequisites

- `libusb`
- `CMake`
- `libuvc-theta`

### Install `libuvc-theta`

```bash
git clone https://github.com/ricohapi/libuvc-theta.git
cd libuvc-theta
mkdir build
cd build
cmake ..
make && sudo make install
```

If you want to enable example and test programs, use:

```bash
cmake .. -DBUILD_TEST=ON -DBUILD_EXAMPLE=ON
```

You can change the build configuration later by editing `CMakeCache.txt` in the build directory or using a CMake GUI.

Then run `./example` or `./uvc_test` from the build directory. Note that `uvc_test` requires OpenCV to build.

## Build

Use the provided `Makefile` in this repository to build the viewer application.

```bash
make
```

## Usage

Connect your RICOH THETA X to the host system via USB and run the built application. The viewer should open a live video stream from the camera.
