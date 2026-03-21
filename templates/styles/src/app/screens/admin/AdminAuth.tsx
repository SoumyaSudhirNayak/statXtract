import { useNavigate } from "react-router";
import { LogIn, UserPlus } from "lucide-react";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";
import { useState, useRef } from "react";

export function AdminAuth() {
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

        <div className="max-w-[1100px] mx-auto px-10 py-24">
          <div className="mb-12">
            <BackButton to="/module-selection" />
          </div>
          
          <div className="text-center mb-16">
            <h2 className="text-6xl text-[#1C1C1C] mb-6 font-black" style={{ fontFamily: 'var(--font-head)' }}>
              Admin Access
            </h2>
            <p className="text-2xl text-[#6B7280] max-w-2xl mx-auto leading-relaxed">
              Sign in to manage the system or create a new administrator account.
            </p>
          </div>

          <div className="grid grid-cols-1 md:grid-cols-2 gap-12">
            {/* Sign In Card */}
            <div
              onClick={() => navigate("/admin/sign-in")}
              onMouseEnter={() => setHoveredCard('signIn')}
              onMouseLeave={() => setHoveredCard(null)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 cursor-pointer transition-all duration-300 relative overflow-hidden flex flex-col items-center text-center shadow-xl min-h-[400px] justify-center"
              style={{
                borderColor: hoveredCard === 'signIn' ? '#2E5BBA' : '#D1D5DB',
                boxShadow: hoveredCard === 'signIn'
                  ? '0 25px 60px rgba(46, 91, 186, 0.25)'
                  : 'none',
              }}
            >
              <div 
                className="w-28 h-28 flex items-center justify-center bg-gradient-to-br from-[#2E5BBA] to-[#1F3A5F] rounded-3xl mb-8 transition-all duration-500 shadow-lg"
                style={{ transform: hoveredCard === 'signIn' ? 'scale(1.1) rotate(5deg)' : 'none' }}
              >
                <LogIn className="w-14 h-14 text-white" />
              </div>
              <h3 className="text-4xl mb-4 font-black" style={{ fontFamily: 'var(--font-head)', color: '#1C1C1C' }}>Sign In</h3>
              <p className="text-xl text-[#6B7280] mb-0 leading-relaxed">Access your admin dashboard and system controls.</p>
            </div>

            {/* Create Account Card */}
            <div
              onClick={() => navigate("/admin/create-account")}
              onMouseEnter={() => setHoveredCard('create')}
              onMouseLeave={() => setHoveredCard(null)}
              className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 cursor-pointer transition-all duration-300 relative overflow-hidden flex flex-col items-center text-center shadow-xl min-h-[400px] justify-center"
              style={{
                borderColor: hoveredCard === 'create' ? '#2E7D32' : '#D1D5DB',
                boxShadow: hoveredCard === 'create'
                  ? '0 25px 60px rgba(46, 125, 50, 0.25)'
                  : 'none',
              }}
            >
              <div 
                className="w-28 h-28 flex items-center justify-center bg-gradient-to-br from-[#2E7D32] to-[#1b5e20] rounded-3xl mb-8 transition-all duration-500 shadow-lg"
                style={{ transform: hoveredCard === 'create' ? 'scale(1.1) rotate(5deg)' : 'none' }}
              >
                <UserPlus className="w-14 h-14 text-white" />
              </div>
              <h3 className="text-4xl mb-4 font-black" style={{ fontFamily: 'var(--font-head)', color: '#1C1C1C' }}>Create Account</h3>
              <p className="text-xl text-[#6B7280] mb-0 leading-relaxed">Register a new administrator for the system.</p>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}