import { useEffect } from "react";
import { useNavigate } from "react-router";
import { motion } from "motion/react";
import { AnimatedBackground } from "../components/AnimatedBackground";

export function SplashScreen() {
  const navigate = useNavigate();

  useEffect(() => {
    const timer = setTimeout(() => {
      navigate("/module-selection");
    }, 2500);

    return () => clearTimeout(timer);
  }, [navigate]);

  return (
    <div className="min-h-screen relative flex items-center justify-center overflow-hidden">
      <AnimatedBackground />
      
      {/* Overlay gradient for depth */}
      <div 
        className="absolute inset-0 z-0"
        style={{
          background: 'radial-gradient(circle at center, transparent 0%, rgba(31, 58, 95, 0.3) 100%)'
        }}
      />
      
      <motion.div
        initial={{ opacity: 0, y: 20, scale: 0.95 }}
        animate={{ opacity: 1, y: 0, scale: 1 }}
        transition={{ duration: 0.8, ease: "easeOut" }}
        className="text-center relative z-10"
      >
        <motion.h1 
          className="text-[#1F3A5F] mb-4"
          style={{ 
            fontFamily: 'var(--font-head)', 
            fontWeight: 900,
            fontSize: '120px',
            letterSpacing: '-0.05em',
            textShadow: '0 10px 40px rgba(31, 58, 95, 0.2)',
          }}
          animate={{
            textShadow: [
              '0 10px 40px rgba(31, 58, 95, 0.2)',
              '0 10px 60px rgba(46, 91, 186, 0.4)',
              '0 10px 40px rgba(31, 58, 95, 0.2)',
            ],
          }}
          transition={{
            duration: 3,
            repeat: Infinity,
            ease: "easeInOut"
          }}
        >
          STATXTRACT
        </motion.h1>
        <motion.p 
          className="text-[#1C1C1C] text-xl"
          style={{ fontFamily: 'var(--font-body)', fontWeight: 400 }}
          initial={{ opacity: 0 }}
          animate={{ opacity: 1 }}
          transition={{ delay: 0.3, duration: 0.5 }}
        >
          Smart Data Access Platform
        </motion.p>
        
        {/* Subtle loading indicator */}
        <motion.div
          className="mt-8 mx-auto w-48 h-1 bg-gradient-to-r from-[#2E5BBA] via-[#F4A300] to-[#2E7D32] rounded-full"
          initial={{ scaleX: 0 }}
          animate={{ scaleX: 1 }}
          transition={{ duration: 2.5, ease: "easeInOut" }}
        />
      </motion.div>
    </div>
  );
}
