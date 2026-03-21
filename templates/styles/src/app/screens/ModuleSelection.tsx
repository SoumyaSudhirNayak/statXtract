import { useNavigate } from "react-router";
import { ShieldCheck, Users, ArrowRight } from "lucide-react";
import { BackButton } from "../components/BackButton";
import { AnimatedBackground } from "../components/AnimatedBackground";
import { useState, useRef } from "react";

export function ModuleSelection() {
  const navigate = useNavigate();
  const [hoveredCard, setHoveredCard] = useState<string | null>(null);

  return (
    <div className="min-h-screen relative">
      <AnimatedBackground />
      
      <div className="relative z-10">
        <div className="bg-[#1F3A5F] text-white px-10 py-6 shadow-xl relative z-50">
          <div className="max-w-[1800px] mx-auto flex justify-between items-center">
            <h1 
              className="text-4xl m-0 font-black tracking-tighter cursor-pointer hover:opacity-80 transition-opacity" 
              style={{ fontFamily: 'var(--font-head)', color: 'white' }}
              onClick={() => navigate("/")}
            >
              STATXTRACT
            </h1>
          </div>
        </div>

        <div className="max-w-[1400px] mx-auto px-10 py-24">
          <div className="text-center mb-20">
            <h2 className="text-6xl text-[#1C1C1C] mb-6" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>
              Select Your Portal
            </h2>
            <p className="text-2xl text-[#6B7280] max-w-3xl mx-auto leading-relaxed">
              Access the STATXTRACT data platform through our specialized modules for administrators and users.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-16 max-w-[1100px] mx-auto">
            {/* Admin Module */}
            <div
              onClick={() => navigate("/admin/auth")}
              onMouseEnter={() => setHoveredCard('admin')}
              onMouseLeave={() => setHoveredCard(null)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 cursor-pointer transition-all duration-300 relative overflow-hidden flex flex-col items-center text-center min-h-[450px] justify-center shadow-xl"
              style={{
                borderColor: hoveredCard === 'admin' ? '#2E5BBA' : '#D1D5DB',
                boxShadow: hoveredCard === 'admin' ? '0 25px 60px rgba(46, 91, 186, 0.25)' : 'none'
              }}
            >
              <div 
                className="w-28 h-28 flex items-center justify-center rounded-3xl mb-8 transition-all duration-500"
                style={{ 
                  backgroundColor: hoveredCard === 'admin' ? '#2E5BBA' : '#F3F4F6',
                  transform: hoveredCard === 'admin' ? 'scale(1.1) rotate(5deg)' : 'none'
                }}
              >
                <ShieldCheck 
                  className="w-14 h-14 transition-colors duration-300" 
                  style={{ color: hoveredCard === 'admin' ? 'white' : '#2E5BBA' }} 
                />
              </div>
              <h3 className="text-4xl mb-4 font-black" style={{ fontFamily: 'var(--font-head)', color: '#1C1C1C' }}>Admin Module</h3>
              <p className="text-xl text-[#6B7280] mb-8 leading-relaxed">System administration, user management, and data ingestion controls.</p>
              <div className="flex items-center gap-3 text-[#2E5BBA] font-bold text-xl">
                Enter Portal <ArrowRight className="w-6 h-6" />
              </div>
            </div>

            {/* User Module */}
            <div
              onClick={() => navigate("/user/module-selection")}
              onMouseEnter={() => setHoveredCard('user')}
              onMouseLeave={() => setHoveredCard(null)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 cursor-pointer transition-all duration-300 relative overflow-hidden flex flex-col items-center text-center min-h-[450px] justify-center shadow-xl"
              style={{
                borderColor: hoveredCard === 'user' ? '#2E7D32' : '#D1D5DB',
                boxShadow: hoveredCard === 'user' ? '0 25px 60px rgba(46, 125, 50, 0.25)' : 'none'
              }}
            >
              <div 
                className="w-28 h-28 flex items-center justify-center rounded-3xl mb-8 transition-all duration-500"
                style={{ 
                  backgroundColor: hoveredCard === 'user' ? '#2E7D32' : '#F3F4F6',
                  transform: hoveredCard === 'user' ? 'scale(1.1) rotate(5deg)' : 'none'
                }}
              >
                <Users 
                  className="w-14 h-14 transition-colors duration-300" 
                  style={{ color: hoveredCard === 'user' ? 'white' : '#2E7D32' }} 
                />
              </div>
              <h3 className="text-4xl mb-4 font-black" style={{ fontFamily: 'var(--font-head)', color: '#1C1C1C' }}>User Module</h3>
              <p className="text-xl text-[#6B7280] mb-8 leading-relaxed">Data exploration, query tools, and personalized analysis dashboards.</p>
              <div className="flex items-center gap-3 text-[#2E7D32] font-bold text-xl">
                Enter Portal <ArrowRight className="w-6 h-6" />
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}