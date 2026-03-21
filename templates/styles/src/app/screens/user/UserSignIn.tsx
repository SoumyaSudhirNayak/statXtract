import { useState, useRef } from "react";
import { useNavigate } from "react-router";
import { BackButton } from "../../components/BackButton";
import { AnimatedBackground } from "../../components/AnimatedBackground";

export function UserSignIn() {
  const navigate = useNavigate();
  const [formData, setFormData] = useState({
    email: "",
    password: "",
  });
  const [hoveredInput, setHoveredInput] = useState<string | null>(null);
  const [isContainerHovered, setIsContainerHovered] = useState(false);
  const containerRef = useRef<HTMLDivElement>(null);

  const handleSubmit = () => {
    // Mock authentication - store user data
    const userData = {
      fullName: "User Name",
      email: formData.email,
    };
    sessionStorage.setItem("userData", JSON.stringify(userData));
    navigate("/user/dashboard");
  };

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

        <div className="max-w-[800px] mx-auto px-6 py-20">
          <div className="mb-10">
            <BackButton to="/user/module-selection" />
          </div>
          
          <h2 className="text-5xl text-[#1C1C1C] mb-12 text-center" style={{ fontFamily: 'var(--font-head)', fontWeight: 800 }}>
            User Sign In
          </h2>

          <div 
            ref={containerRef}
            onMouseEnter={() => setIsContainerHovered(true)}
            onMouseLeave={() => setIsContainerHovered(false)}
            className="lift-on-hover bg-white/80 backdrop-blur-sm border-2 rounded-2xl p-12 relative overflow-hidden shadow-2xl"
            style={{
              borderColor: isContainerHovered ? '#2E7D32' : '#D1D5DB',
              boxShadow: isContainerHovered 
                ? '0 30px 70px rgba(46, 125, 50, 0.25)'
                : 'none',
            }}
          >
            <div className="space-y-10 relative z-10">
              <div>
                <label className="block text-[#1C1C1C] mb-4 text-xl font-bold">Email Address</label>
                <input
                  type="email"
                  value={formData.email}
                  onChange={(e) => setFormData({ ...formData, email: e.target.value })}
                  onMouseEnter={() => setHoveredInput('email')}
                  onMouseLeave={() => setHoveredInput(null)}
                  placeholder="Enter your email"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                  style={{
                    borderColor: hoveredInput === 'email' ? '#2E7D32' : '#D1D5DB',
                    boxShadow: hoveredInput === 'email' ? '0 0 15px rgba(46, 125, 50, 0.2)' : 'none'
                  }}
                />
              </div>

              <div>
                <label className="block text-[#1C1C1C] mb-4 text-xl font-bold">Password</label>
                <input
                  type="password"
                  value={formData.password}
                  onChange={(e) => setFormData({ ...formData, password: e.target.value })}
                  onMouseEnter={() => setHoveredInput('password')}
                  onMouseLeave={() => setHoveredInput(null)}
                  placeholder="••••••••"
                  className="w-full px-6 py-4 border-2 rounded-xl focus:outline-none focus:border-[#F4A300] bg-white transition-all duration-300 text-xl shadow-inner"
                  style={{
                    borderColor: hoveredInput === 'password' ? '#2E7D32' : '#D1D5DB',
                    boxShadow: hoveredInput === 'password' ? '0 0 15px rgba(46, 125, 50, 0.2)' : 'none'
                  }}
                />
              </div>

              <button
                onClick={handleSubmit}
                onMouseEnter={() => setHoveredInput('submit')}
                onMouseLeave={() => setHoveredInput(null)}
                className="lift-on-hover w-full bg-[#2E7D32] text-white py-5 rounded-xl border-2 cursor-pointer transition-all duration-300 text-2xl font-black shadow-lg mt-4"
                style={{
                  borderColor: hoveredInput === 'submit' ? '#F4A300' : '#2E7D32',
                  boxShadow: hoveredInput === 'submit' ? '0 15px 40px rgba(244, 163, 0, 0.4)' : 'none'
                }}
              >
                Sign In to Account
              </button>

              <div className="text-center mt-4">
                <p className="text-[#6B7280] text-sm m-0">
                  Don't have an account?{" "}
                  <button 
                    onClick={() => navigate("/user/create-account")}
                    className="bg-transparent border-none text-[#2E5BBA] cursor-pointer font-medium hover:underline p-0"
                  >
                    Create Account
                  </button>
                </p>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
  );
}