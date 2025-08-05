CC = gcc
CFLAGS = -pthread $(shell pkg-config --cflags gstreamer-1.0 gstreamer-app-1.0 libuvc)
LIBS = $(shell pkg-config --libs gstreamer-1.0 gstreamer-app-1.0 libuvc) -lpthread

TARGET = file_saver
OBJS = file_saver.o

$(TARGET): $(OBJS)
	$(CC) $(OBJS) -o $(TARGET) $(LIBS)

%.o: %.c
	$(CC) $(CFLAGS) -c $< -o $@

clean:
	rm -f *.o $(TARGET)

run: $(TARGET)
	./$(TARGET)

list:
	./$(TARGET) -l

.PHONY: clean run list