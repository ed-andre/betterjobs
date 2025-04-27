import React from 'react';
import { Routes, Route } from 'react-router-dom';
import Box from '@mui/material/Box';
import Header from './components/Header';
import Home from './pages/Home';
import JobSearch from './pages/JobSearch';
import JobDetails from './pages/JobDetails';
import Companies from './pages/Companies';
import Stats from './pages/Stats';

function App() {
  return (
    <Box sx={{ display: 'flex', flexDirection: 'column', minHeight: '100vh' }}>
      <Header />
      <Box component="main" sx={{ flexGrow: 1, py: 3 }}>
        <div className="container">
          <Routes>
            <Route path="/" element={<Home />} />
            <Route path="/search" element={<JobSearch />} />
            <Route path="/job/:id" element={<JobDetails />} />
            <Route path="/companies" element={<Companies />} />
            <Route path="/stats" element={<Stats />} />
          </Routes>
        </div>
      </Box>
    </Box>
  );
}

export default App;