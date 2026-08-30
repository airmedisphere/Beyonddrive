# Use the official Python base image
FROM python:3.11.7-slim AS base

# Set the working directory inside the container
WORKDIR /app

# Copy the requirements file to the working directory and install dependencies
COPY requirements.txt .
# ffmpeg: video transcoding/streaming
# calibre: converts MOBI/AZW3 -> EPUB for the in-browser book reader
# djvulibre-bin: converts DJVU -> PDF for the in-browser book reader, and
#   also used directly to render DJVU page 1 for auto-generated covers
# fonts-dejavu-core: text font used when drawing generated title-card
#   covers (PDF/embedded-cover extraction failed, or format is TXT)
# NOTE: calibre pulls in a lot of dependencies and noticeably increases
# image size / build time. If you don't need MOBI/AZW3/DJVU reader support,
# you can drop `calibre djvulibre-bin` from this line and those formats
# will simply fall back to download-only in the UI (and auto-generated
# covers for them will fall back to a generated title card).
RUN apt-get update && \
    DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends \
        ffmpeg calibre djvulibre-bin fonts-dejavu-core && \
    rm -rf /var/lib/apt/lists/* && \
    pip install --no-cache-dir -r requirements.txt

# Copy the application code to the working directory
COPY . .

# Expose the port on which the application will run
EXPOSE 8080

# Run the FastAPI application using uvicorn server
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]
