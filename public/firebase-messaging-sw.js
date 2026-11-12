/* Firebase background notifications for Gigi Chat */
importScripts('https://www.gstatic.com/firebasejs/10.14.1/firebase-app-compat.js');
importScripts('https://www.gstatic.com/firebasejs/10.14.1/firebase-messaging-compat.js');

firebase.initializeApp({
  apiKey: 'AIzaSyAyV_xX052tKZi26yWznkJ9FOAPXNY55U0',
  authDomain: 'gigi-chat-c743a.firebaseapp.com',
  projectId: 'gigi-chat-c743a',
  storageBucket: 'gigi-chat-c743a.firebasestorage.app',
  messagingSenderId: '737635454927',
  appId: '1:737635454927:web:faa1a791657a446f124cba',
  measurementId: 'G-Y3H9KD33Y6'
});

const messaging = firebase.messaging();
messaging.onBackgroundMessage(payload => {
  const title = payload.notification?.title || payload.data?.title || 'Gigi Chat';
  const options = {
    body: payload.notification?.body || payload.data?.body || 'Нове повідомлення',
    icon: '/favicon.ico',
    data: { url: payload.data?.url || '/' }
  };
  self.registration.showNotification(title, options);
});

self.addEventListener('notificationclick', event => {
  event.notification.close();
  const url = event.notification.data?.url || '/';
  event.waitUntil(clients.matchAll({ type: 'window', includeUncontrolled: true }).then(list => {
    for (const client of list) { if ('focus' in client) { client.navigate(url); return client.focus(); } }
    return clients.openWindow(url);
  }));
});
