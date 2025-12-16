import React from 'react'
import ReactDOM from 'react-dom/client'
import App from './App.jsx'
import '@aws-amplify/ui-react/styles.css'
import { Amplify } from 'aws-amplify'
import './index.css'

// Config Cognito
Amplify.configure({
  Auth: {
    Cognito: {
      userPoolId: 'ap-southeast-1_LZU2SXqyz',
      userPoolClientId: '6v29eis60gqjicijhirvp22of',
      loginWith: {
        email: true,
        username: false
      }
    }
  },
  oauth: {
    domain: 'ap-southeast-1lzu2sxqyz.auth.ap-southeast-1.amazoncognito.com',
    scope: ['email', 'openid', 'profile'],
    redirectSignIn: 'http://localhost:5173/',
    redirectSignOut: 'http://localhost:5173/',
    responseType: 'code'
  }
})

ReactDOM.createRoot(document.getElementById('root')).render(
  <React.StrictMode>
    <App />
  </React.StrictMode>
)