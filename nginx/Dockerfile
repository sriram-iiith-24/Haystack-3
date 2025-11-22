FROM nginx:alpine

# Remove default config
RUN rm /etc/nginx/conf.d/default.conf

# Copy our config
COPY nginx.conf /etc/nginx/conf.d/

# Expose port
EXPOSE 80

CMD ["nginx", "-g", "daemon off;"]